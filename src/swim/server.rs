


use std::sync::{Arc, Mutex};

use actix_web::web::Data;
use actix_web::{get, put, web, App, HttpRequest, HttpServer, HttpResponse};


use log::info;

use serde::Deserialize;


use crate::HolyDiverController;









#[derive(Deserialize)]
struct FieldUpdate {
    value: String,
}

#[get("/hello")]
async fn hello(req:HttpRequest) -> &'static str {
    info!("REQ: {:?}", req);
    "Hello world!\r\n"
}

#[get("/state/{field}")]
async fn get_field(field:web::Path<String>
    , controller:web::Data<Arc<Mutex<HolyDiverController>>>) -> HttpResponse {
    let field_value = controller.lock().unwrap().get_field(field.to_string());
    info!("Got field value: {:?}", field_value);
    HttpResponse::Ok().body(format!("{}: {}", field, field_value.unwrap_or("N/A".to_owned())))
}

#[put("/state/{field}")]
async fn update_field(field:web::Path<String>
    , web::Json(update): web::Json<FieldUpdate>
    , controller:web::Data<Arc<Mutex<HolyDiverController>>>) -> HttpResponse {
        //TODO somehow get foca or another handler here to be able to publish a broadcast
        // would be better to just make a trait for every component and then figure out how to pass things around
    controller.lock().unwrap().set_field(field.to_string(), update.value).await.unwrap();
    HttpResponse::Ok().finish()
}
// same as fn host_server<T: HolyDiverController + Send + Sync>(port:u16, controller:&T)
pub async fn host_server(port: u16, controller: Arc<Mutex<HolyDiverController>>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
        .app_data(Data::new(controller.clone()))
        .service(hello)
        .service(get_field)
        .service(update_field)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}

// pub struct HolyDiverRestController {
//     foca_command_sender: Sender<FocaCommand>,
//     local_state: Mutex<AutoCommit>,
// }

// impl HolyDiverController for HolyDiverRestController {
//     fn get_field(&self, field_name: String) -> Result<String> {
//         let state = self.local_state.get_mut().unwrap();

//         //TODO fix this
//         let field_value = state.get(ROOT, field_name)?
//             .map(|v| v.0)
//             .ok_or_else(|| 0)
//             .unwrap()
//             .into_string()
//             .unwrap();
//         Ok(field_value)
//     }

//     fn set_field(&self, field_name: String, field_value: String) -> Result<()> {
//         let state = self.local_state.get_mut().unwrap();
//         state.put(ROOT, field_name, field_value)?;
//         let broadcast_msg = craft_broadcast(SyncOperation {
//             operation_id: Uuid::new_v4()
//         }, GossipMessage::new(FullSync, state.save()));
//         // broadcasting the change so that all nodes get this update
//         self.foca_command_sender.send(FocaCommand::SendBroadcast((SyncOperation {
//             operation_id: Uuid::new_v4()
//         }, GossipMessage::new(FullSync, state.save()))));
//         // self.foca.add_broadcast(broadcast_msg.as_ref())?;
//         Ok(())
//     }
// }