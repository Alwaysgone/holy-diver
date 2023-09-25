use std::sync::{Arc, Mutex};

use actix_web::{get, put, web, App, HttpRequest, HttpServer, HttpResponse};
use automerge::{AutomergeError, AutoCommit, ROOT, ReadDoc, transaction::Transactable};
use foca::{Foca, PostcardCodec};
use log::info;
use rand::rngs::StdRng;
use serde::Deserialize;
use tracing::field;
use uuid::Uuid;
use crate::{MyDataHandler, swim::broadcast::GossipMessage};

use super::{core::HolyDiverController, types::ID, broadcast::Handler};
use crate::swim::broadcast::MessageType::FullSync;
use crate::swim::broadcast::Tag::SyncOperation;
use anyhow::Result;

#[derive(Deserialize)]
struct FieldUpdate {
    value: String,
}

#[get("/hello")]
async fn hello(req:HttpRequest) -> &'static str {
    info!("REQ: {:?}", req);
    "Hello world!\r\n"
}

#[put("/state/{field}")]
async fn update_field(field:web::Path<String>
    , web::Json(update): web::Json<FieldUpdate>
    , data_handler:web::Data<Arc<MyDataHandler>>) -> HttpResponse {
        //TODO somehow get foca or another handler here to be able to publish a broadcast
        // would be better to just make a trait for every component and then figure out how to pass things around

    HttpResponse::Ok().finish()
}
// same as fn host_server<T: HolyDiverController + Send + Sync>(port:u16, controller:&T)
pub async fn host_server(port: u16, controller: Arc<dyn HolyDiverController + Send + Sync>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
        .app_data(controller.clone())
        .service(hello)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}

pub struct HolyDiverRestController {
    foca: Foca<ID, PostcardCodec, StdRng, Handler>,
    handler: Handler,
    local_state: Mutex<AutoCommit>,
}

impl HolyDiverController for HolyDiverRestController {
    fn get_field(&mut self, field_name: String) -> Result<String> {
        let state = self.local_state.get_mut().unwrap();

        //TODO fix this
        let field_value = state.get(ROOT, field_name)?
            .map(|v| v.0)
            .ok_or_else(|| 0)
            .unwrap()
            .into_string()
            .unwrap();
        Ok(field_value)
    }

    fn set_field(&mut self, field_name: String, field_value: String) -> Result<()> {
        let state = self.local_state.get_mut().unwrap();
        state.put(ROOT, field_name, field_value)?;
        let broadcast_msg = self.handler.craft_broadcast(SyncOperation {
            operation_id: Uuid::new_v4()
        }, GossipMessage::new(FullSync, state.save()));
        // broadcasting the change so that all nodes get this update
        self.foca.add_broadcast(broadcast_msg.as_ref())?;
        Ok(())
    }
}