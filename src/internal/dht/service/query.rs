use log::warn;
use tonic::{Response, Status};

use super::super::{grpc::*, node::Node};

use crate::internal::util::{self};

impl Node {
    pub async fn query_service(
        &self,
        req: &QueryRequest,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        if let Some(node) = self.route_with_leaf_set(req.key).await {
            if node.id == self.id {
                // Node is the owner of key
                return match self.execute_query(req).await {
                    Ok(value) => Ok(Response::new(QueryResponse {
                        from_id: self.id,
                        hops: req.hops,
                        key: req.key,
                        value,
                        error: None,
                    })),
                    Err(err) => {
                        warn!("#{:016X}: Query error: {}", self.id, err);

                        Ok(Response::new(QueryResponse {
                            from_id: self.id,
                            hops: req.hops,
                            key: req.key,
                            value: None,
                            error: Some(
                                match QueryType::from_i32(req.query_type).unwrap() {
                                    QueryType::Set => QueryError::ValueNotProvided,
                                    QueryType::Get | QueryType::Delete => QueryError::KeyNotFound,
                                }
                                .into(),
                            ),
                        }))
                    }
                };
            }
        }

        let mut request = req.clone();
        request.from_id = self.id;
        request.matched_digits = util::get_num_matched_digits(self.id, req.key)?;
        request.hops += 1;

        if let Some(res) = self.query_with_leaf_set(&request).await? {
            return Ok(res);
        }

        if let Some(res) = self.query_with_routing_table(&request).await? {
            return Ok(res);
        }

        self.query_with_closest_from_leaf_set(&request).await
    }

    // QUERY
    async fn query_with_leaf_set(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Option<Response<QueryResponse>>, Status> {
        loop {
            let node = match self.route_with_leaf_set(request.key).await {
                Some(node) => node,
                None => return Ok(None),
            };

            match self.connect_and_query(&node, request.clone()).await {
                Ok(r) => break Ok(Some(r)),
                Err(err) => self.warn_and_fix_leaf_entry(&node, &err.to_string()).await,
            }
        }
    }

    async fn query_with_routing_table(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Option<Response<QueryResponse>>, Status> {
        let (node, _) = match self
            .route_with_routing_table(request.key, request.matched_digits as usize)
            .await
        {
            Some(res) => res,
            None => return Ok(None),
        };

        if node.id == self.id {
            return Ok(None);
        }

        match self.connect_and_query(&node, request.clone()).await {
            Ok(r) => return Ok(Some(r)),
            Err(err) => self.warn_and_fix_table_entry(&node, &err.to_string()).await,
        }

        Ok(None)
    }

    async fn query_with_closest_from_leaf_set(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        loop {
            let (node, _) = self.get_closest_from_leaf_set(request.key).await;

            match self.connect_and_query(&node, request.clone()).await {
                Ok(r) => break Ok(r),
                Err(err) => self.warn_and_fix_leaf_entry(&node, &err.to_string()).await,
            }
        }
    }
}
