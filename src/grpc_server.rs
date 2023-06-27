use crate::{change::Change, pgpool::PgPool};
use futures::lock::Mutex;
use reducerapis_proto::{
    reducer_apis_server::{ReducerApis, ReducerApisServer},
    GetObjectRequest, GetObjectResponse,
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct GrpcServer {
    pool: Arc<PgPool>,
    msgrx: Arc<broadcast::Receiver<Arc<(i64, Vec<Change>)>>>,
    last_idx: Arc<Mutex<i64>>,
}

impl GrpcServer {
    fn new(
        last_idx: i64,
        pool: Arc<PgPool>,
        msgrx: broadcast::Receiver<Arc<(i64, Vec<Change>)>>,
    ) -> Self {
        Self {
            last_idx: Arc::new(Mutex::new(last_idx)),
            pool,
            msgrx: Arc::new(msgrx),
        }
    }

    async fn setidx(&self, idx: i64) {
        let mut guard = self.last_idx.lock().await;
        *guard = idx;
    }
}

#[tonic::async_trait]
impl ReducerApis for GrpcServer {
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let GetObjectRequest {
            object_type,
            object_id,
            idx,
        } = request.into_inner();
        if let Some(idx) = idx {
            let mut msgrx = self.msgrx.resubscribe();
            if self.last_idx.lock().await.gt(&idx) {
                loop {
                    let msg = msgrx.recv().await.unwrap();
                    if msg.0 == idx {
                        break;
                    }
                }
            }
        }
        use anystruct::IntoProto;
        let result = self
            .pool
            .get_object(object_type, object_id, idx)
            .await
            .unwrap();
        let (idx, value) = match result {
            None => (None, None),
            Some((idx, None)) => (Some(idx), None),
            Some((idx, Some(value))) => (Some(idx), Some(value.into_proto())),
        };
        let response = GetObjectResponse {
            idx,
            object_data: value,
        };
        Ok(Response::new(response))
    }
}

pub async fn start(
    last_idx: i64,
    msgrx: broadcast::Receiver<Arc<(i64, Vec<Change>)>>,
    pool: Arc<PgPool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:40001".parse()?;
    let (tx, rx) = broadcast::channel(32);
    let server = GrpcServer::new(last_idx, pool, rx);
    tokio::spawn({
        let mut msgrx = msgrx;
        let server = server.clone();
        async move {
            loop {
                let value = msgrx.recv().await.unwrap();
                let idx = value.0;
                tx.send(value).unwrap();
                server.setidx(idx).await;
            }
        }
    });
    Server::builder()
        .add_service(ReducerApisServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
