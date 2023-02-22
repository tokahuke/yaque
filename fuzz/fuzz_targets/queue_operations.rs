#![no_main]

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tempfile::TempDir;
use yaque::{channel, TryRecvError};
use std::collections::VecDeque;


fuzz_target!(|scenario: Scenario| { scenario.execute() });

#[derive(Arbitrary, Debug)]
struct Scenario {
    commands: Vec<QueueCommand>,
}

impl Scenario {
    pub fn execute(&self) {
        let base_dir = std::env::var("YAQUE_FUZZ_BASE_DIR").expect(
            "You must set the YAQUE_FUZZ_BASE_DIR environment variable, preferrably to a ramdisk",
        );
        let queue_dir = TempDir::new_in(base_dir).unwrap();

        let (mut tx, mut rx) = channel(&queue_dir).unwrap();
        let mut pending_sent_items: VecDeque<Vec<u8>> = Default::default();

        for command in self.commands.iter() {
            match command {
                QueueCommand::Send(buffer) => {
                    tx.try_send(buffer).expect("Send");
                    pending_sent_items.push_back(buffer.clone());
                }
                QueueCommand::SendBatch(buffers) =>  {
                    tx.try_send_batch(buffers).expect("SendBatch");
                    pending_sent_items.extend(buffers.clone());
                }
                QueueCommand::Recv =>  {
                    match rx.try_recv() {
                        Ok(item) => {
                            let item = item.try_into_inner().unwrap();
                            let pending = pending_sent_items.pop_front().unwrap();
                            assert_eq!(pending, item);
                        }
                        Err(TryRecvError::Io(io)) => panic!("{io:?}"),
                        Err(TryRecvError::QueueEmpty) => ()
                    }
                }
                QueueCommand::RecvBatch(count) =>  {
                    match rx.try_recv_batch(*count) {
                        Ok(items) =>  {
                            let items = items.try_into_inner().unwrap();
                            for item in items.into_iter() {
                                let pending = pending_sent_items.pop_front().unwrap();
                                assert_eq!(pending, item);
                            }
                        }
                        Err(TryRecvError::Io(io)) => panic!("{io:?}"),
                        Err(TryRecvError::QueueEmpty) => ()
                    }
                }
            }
        }
    }
}

#[derive(Arbitrary, Debug)]
enum QueueCommand {
    Send(Vec<u8>),
    SendBatch(Vec<Vec<u8>>),
    Recv,
    RecvBatch(usize),
}
