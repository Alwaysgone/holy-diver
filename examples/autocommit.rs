use automerge::{ObjType, AutoCommit, transaction::{Transactable, Observed},
    ReadDoc, AutoSerde, AutoCommitWithObs, VecOpObserver, AutomergeError,
    sync::{self, SyncDoc}};

fn main() -> Result<(), AutomergeError> {
    //Should record the changes? dunno how to get it back yet, from doc or changes call take_patches
    let alice_observer = VecOpObserver::default();

    //Initializes document at alice's side
    let mut alice = AutoCommit::new()
        .with_observer(alice_observer);
    
    //Create counter scalar that can be used with AutoCommit::increment
    alice.put(automerge::ROOT, "counter", automerge::ScalarValue::counter(0))?;
    //Increment counter by one
    //Will look like this:
    // {
    //   "counter": 1
    // }
    alice.increment(automerge::ROOT, "counter", 1)?;
    
    print(&alice, "Alice's initital state");

    let mut alices_state = sync::State::new();
    let sync_msg_to_bob = alice.sync().generate_sync_message(&mut alices_state).unwrap();

    let bob_observer = VecOpObserver::default();
    let mut bob = AutoCommit::new()
        .with_observer(bob_observer);
    print(&bob, "Bob's initial state");

    //syncing adjusted from https://automerge.org/automerge/automerge/sync/#example
    let mut bobs_state = sync::State::new();
    //why this extra initial sync?
    bob.sync().receive_sync_message(&mut bobs_state, sync_msg_to_bob)?;

    //IMPORTANT syncing is for one-on-one communication
    //          good for bootstrapping
    loop {
        let bob_to_alice = bob.sync().generate_sync_message(&mut bobs_state);
        if let Some(message) = bob_to_alice.as_ref() {
            println!("syncing from bob to alice");
            alice.sync().receive_sync_message(&mut alices_state, message.clone())?;
        }
        let alice_to_bob = alice.sync().generate_sync_message(&mut alices_state);
        if let Some(message) = alice_to_bob.as_ref() {
            println!("syncing from alice to bob");
            bob.sync().receive_sync_message(&mut bobs_state, message.clone())?;
        }
        if bob_to_alice.is_none() && alice_to_bob.is_none() {
            println!("syncing complete");
            break;
        }
    }

    alice.save();
    bob.save();
    print(&bob, "Bob's state after initial sync");
    
    alice.increment(automerge::ROOT, "counter", 3)?;
    print(&alice, "Alice incremented by 3");

    bob.increment(automerge::ROOT, "counter", 10)?;
    print(&bob, "Bob incremented by 10");

    let alice_change1 = alice.save_incremental();
    println!("\"Sending\" {} bytes from alice to bob ...", alice_change1.capacity());
    bob.load_incremental(&alice_change1)?;
    print(&bob, "Bob received incremental update from alice");

    print(&alice, "Alice current state before receiving any update from bob");

    let bob_change1 = bob.save_incremental();
    println!("\"Sending\" {} bytes from alice to bob ...", bob_change1.capacity());
    alice.load_incremental(&bob_change1)?;
    print(&alice, "Alice received incremental update from bob");

    Ok(())
}

fn print(doc: &AutoCommitWithObs<Observed<VecOpObserver>>, txt: &str) {
    let serialized = serde_json::to_string_pretty(&automerge::AutoSerde::from(doc)).unwrap();
    println!("{}", txt);
    println!("{}", serialized);
    println!();
}