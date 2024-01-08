use crdts::{CmRDT, CvRDT, PNCounter};
use distributed::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: i64 },
    AddOk,
    Read,
    ReadOk { value: i64 },
    Gossip { json: String },
}

enum InjectedPayload {
    Gossip,
}

struct GrowCounterNode {
    node: String,
    id: usize,
    counter: PNCounter<String>,
    others: Vec<String>,
    tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
}

impl Node<(), Payload, InjectedPayload> for GrowCounterNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node: init.node_id.clone(),
            id: 1,
            counter: PNCounter::new(),
            others: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    let node_counter =
                        serde_json::to_string(&self.counter).expect("Serialization error");
                    for n in &self.others {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip {
                                    json: node_counter.clone(),
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Gossip { json } => {
                        let other_counter: PNCounter<String> = serde_json::from_str(&json).unwrap();
                        self.counter.merge(other_counter);
                    }
                    Payload::Add { delta } => {
                        if delta != 0 {
                            if delta > 0 {
                                self.counter
                                    .apply(self.counter.inc_many(self.node.clone(), delta as u64));
                            } else {
                                self.counter.apply(
                                    self.counter.dec_many(self.node.clone(), (-delta) as u64),
                                );
                            }
                            self.tx.send(Event::Injected(InjectedPayload::Gossip))?;
                        }

                        reply.body.payload = Payload::AddOk;
                        reply.send(output).context("send response to add")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            value: self.counter.read().to_string().parse::<i64>().unwrap(),
                        };
                        reply.send(output).context("send response to read")?;
                    }
                    Payload::AddOk | Payload::ReadOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, GrowCounterNode, _, _>(())
}
