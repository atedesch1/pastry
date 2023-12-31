use log::{info, warn};
use tonic::{Response, Status};

use super::super::node::Node;
use super::grpc::*;

use crate::{
    error::*,
    internal::{
        dht::node::NodeInfo,
        util::{self},
    },
};

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

    async fn connect_and_query(
        &self,
        node: &NodeInfo,
        request: QueryRequest,
    ) -> Result<Response<QueryResponse>> {
        match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
            Ok(mut client) => Ok(client.query(request.clone()).await?),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn execute_query(&self, query: &QueryRequest) -> Result<Option<Vec<u8>>> {
        let key = &query.key;
        let value = &query.value;
        let query_type = query.query_type;

        info!(
            "#{:016X}: Executing query for key {:016X}",
            self.id, query.key
        );

        match QueryType::try_from(query_type).unwrap() {
            QueryType::Set => match value {
                None => Err(Error::Value("Value not provided".into())),
                Some(value) => Ok(self.state.store.write().await.set(key, value)),
            },
            QueryType::Get => match self.state.store.read().await.get(key) {
                None => Err(Error::Value("Key not present in database".into())),
                Some(value) => Ok(Some(value.clone())),
            },
            QueryType::Delete => match self.state.store.write().await.delete(key) {
                None => Err(Error::Value("Key not present in database.".into())),
                Some(value) => Ok(Some(value)),
            },
        }
    }
}
