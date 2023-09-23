use std::sync::Arc;

use actix_web::{get, put, web, App, HttpRequest, HttpServer, HttpResponse};
use log::info;
use serde::Deserialize;
use crate::MyDataHandler;

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

pub async fn host_server(port:u16, data_handler:Arc<MyDataHandler>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
        .app_data(data_handler.clone())
        .service(hello)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}