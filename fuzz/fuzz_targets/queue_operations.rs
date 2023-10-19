#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use log::trace;
use std::collections::VecDeque;
use tempfile::TempDir;
use yaque::{channel, TryRecvError};

fuzz_target!(|scenario: Scenario| scenario.execute());

#[derive(Arbitrary, Debug)]
struct Scenario {
    commands: Vec<QueueCommand>,
}

impl Scenario {
    pub fn execute(&self) {
        //simple_logger::SimpleLogger::new().env().init().unwrap();

        let base_dir = std::env::var("YAQUE_FUZZ_BASE_DIR").expect(
            "You must set the YAQUE_FUZZ_BASE_DIR environment variable, preferrably to a ramdisk",
        );
        let queue_dir = TempDir::new_in(base_dir).unwrap();

        let (mut tx, mut rx) = channel(&queue_dir).unwrap();
        let mut pending_sent_items: VecDeque<Vec<u8>> = Default::default();

        for (n, command) in self.commands.iter().enumerate() {
            trace!("Command {}: {command:?}", n + 1);

            match command {
                QueueCommand::Send(msg) => {
                    tx.try_send(msg.0.clone()).expect("Send");
                    pending_sent_items.push_back(msg.0.clone());
                }
                QueueCommand::SendBatch(msgs) => {
                    let buffers = msgs.iter().map(|m| m.0.clone()).collect::<Vec<_>>();
                    tx.try_send_batch(buffers.clone()).expect("SendBatch");
                    pending_sent_items.extend(buffers);
                }
                QueueCommand::Recv => match rx.try_recv() {
                    Ok(item) => {
                        let item = item.try_into_inner().unwrap();
                        let pending = pending_sent_items.pop_front().unwrap();
                        assert_eq!(pending, item);
                    }
                    Err(TryRecvError::Io(io)) => panic!("{io:?}"),
                    Err(TryRecvError::QueueEmpty) => {
                        assert!(pending_sent_items.is_empty());
                    }
                },

                // QueueCommand::RecvBatch(count) => match rx.try_recv_batch(*count as usize) {
                //     Ok(items) => {
                //         assert!(
                //             *count as usize <= pending_sent_items.len(),
                //             "Should get data when batch is is <= available count"
                //         );
                //         let items = items.try_into_inner().unwrap();
                //         for item in items.into_iter() {
                //             let pending = pending_sent_items.pop_front().unwrap();
                //             assert_eq!(pending, item);
                //         }
                //     }
                //     Err(TryRecvError::Io(io)) => panic!("{io:?}"),
                //     Err(TryRecvError::QueueEmpty) => {
                //         trace!(
                //             "got QueueEmpty. count: {}, pending_sent_items.len(): {}",
                //             &count,
                //             pending_sent_items.len()
                //         );
                //         assert!(
                //             *count as usize > pending_sent_items.len(),
                //             "Should see QueueEmpty when batch size > available count"
                //         )
                //     }
                // },

                QueueCommand::RecvBatchUpTo(count) => {
                    match rx.try_recv_batch_up_to(*count as usize) {
                        Ok(items) => {
                            let items = items.try_into_inner().unwrap();
                            for item in items.into_iter() {
                                let pending = pending_sent_items.pop_front().unwrap();
                                assert_eq!(pending, item);
                            }
                        }
                        Err(TryRecvError::Io(io)) => panic!("{io:?}"),
                        Err(TryRecvError::QueueEmpty) => {
                            assert!(pending_sent_items.is_empty());
                        }
                    }
                }
            }
        }
    }
}

#[derive(Arbitrary, Debug)]
enum QueueCommand {
    Send(NonEmptyMsg),
    SendBatch(Vec<NonEmptyMsg>),
    Recv,
    //RecvBatch(u16),
    RecvBatchUpTo(u16),
}

#[derive(Debug)]
struct NonEmptyMsg(Vec<u8>);

impl<'a> Arbitrary<'a> for NonEmptyMsg {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        let mut msg = vec![u8::arbitrary(u)?];
        msg.extend(Vec::<u8>::arbitrary(u)?);
        Ok(NonEmptyMsg(msg))
    }
}
