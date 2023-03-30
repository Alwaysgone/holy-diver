use automerge::ReadDoc;
use automerge::sync::State;
use automerge::sync::SyncDoc;
use automerge::transaction::Transactable;
use automerge::AutomergeError;
use automerge::ObjType;
use automerge::{Automerge, ROOT, AutoCommit};
use std::fs::File;
use std::io::Write;

fn main() {
    let mut data = Automerge::new();
    let heads = data.get_heads();
    let (memo1, memo2) = data.transact::<_,_,AutomergeError>(|tx| {
        let memos = tx.put_object(ROOT, "memos", ObjType::Map).unwrap();
        let memo1 = tx.put(&memos, "Memo1", "Do the thing").unwrap();
        let memo2 = tx.put(&memos, "Memo2", "Add automerge support").unwrap();
        Ok((memo1, memo2))
    })
    .unwrap()
    .result;


    // let a = data.save();
    // println!("Memo1: {:?}, Memo2: {:?}", memo1, memo2);
    let changes = data.get_changes(&heads).unwrap();
    // println!("Changes since start:");
    // changes.iter().for_each(|c| println!("{:?}", c));

    // for syncing see https://docs.rs/automerge/0.3.0/automerge/sync/index.html

    let mut commit_data = AutoCommit::new();
    let mut state = State::new();
    let mut state_file = File::create("./data/state.txt").unwrap();
    let mut data_file = File::create("./data/data.txt").unwrap();    
    let memos = commit_data.put_object(ROOT, "memos", ObjType::Map).unwrap();
    commit_data.put(&memos, "Memo1", "Do the thing").unwrap();
    commit_data.put(&memos, "Memo2", "Add automerge support").unwrap();
    data_file.write_all(&commit_data.save()).unwrap();  
    let sync_msg_1 = commit_data.sync().generate_sync_message(&mut state);

    let encoded_state = state.encode();
    state_file.write_all(&encoded_state).unwrap();
    // println!("Stored state: {:?}", state);
    // println!("Current data: {:?}", commit_data);
    println!("Sync message to send: {:?}", sync_msg_1);

    let memos_opt = commit_data.get(ROOT, "memos").unwrap();
    let memos_id = memos_opt.clone().unwrap().1;
    let previous_memo = commit_data.get(&memos_id, "Memo2").unwrap();
    println!("Previous Memo2: {}", previous_memo.unwrap().0);
    commit_data.put(&memos_id, "Memo2", "Added atomerge support").unwrap();
    data_file.write_all(&commit_data.save()).unwrap();  
    let sync_msg_2 = commit_data.sync().generate_sync_message(&mut state);
    
    let new_memo = commit_data.get(&memos_id, "Memo2").unwrap();
    println!("New Memo2: {}", new_memo.unwrap().0);
    let encoded_state2 = state.encode();
    println!("Encoded state length: {}", encoded_state2.len());
    state_file.write_all(&encoded_state2).unwrap();
    // println!("Stored state: {:?}", state);
    // println!("Current data: {:?}", commit_data);
    println!("Sync message to send: {:?}", sync_msg_2);
    // commit_data.get
    // let read_memos = data.get(ROOT, "memos").unwrap();
    //TODO check if state needs to be persisted as well or only the doc
}