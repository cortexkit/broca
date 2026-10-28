//! Replies shaped like the AFT tool provider's encoder, isolated from management-surface modes.

use std::collections::HashMap;

use serde_json::{json, Value};
use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole, Tool};
use subc_protocol::Frame;

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";

// "legacy" is a provider without a catalog result-format declaration; "v1" opts in.
pub fn enabled() -> bool {
    std::env::var_os("SURFACE_TOOL_RESULT").is_some()
}

fn declares_v1() -> bool {
    std::env::var("SURFACE_TOOL_RESULT").as_deref() == Ok("v1")
}

pub fn manifest(module_id: &str) -> ModuleManifest {
    ModuleManifest::builder(module_id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "read".into(),
                description: Some("Read a text or image fixture".into()),
                execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                schema: schema(),
            }],
            identity_scope: Vec::new(),
            concurrency: Concurrency::Serial,
            emits_push: false,
            sub_supervises: false,
        }])
        .build()
}

fn schema() -> Value {
    json!({"type":"object","properties":{"fixture":{"type":"string","enum":["text","image","error"]}},"required":["fixture"]})
}

#[derive(Default)]
pub struct Routes(HashMap<(u16, u32), bool>);

impl Routes {
    pub fn bind(&mut self, frame: &Frame) -> Result<(), String> {
        let bind: Value = serde_json::from_slice(&frame.body).map_err(|e| e.to_string())?;
        let channel = bind["route_channel"]
            .as_u64()
            .ok_or("missing route channel")? as u16;
        let epoch = bind["epoch"].as_u64().ok_or("missing route epoch")? as u32;
        let v1 = declares_v1()
            && bind["consumer_capabilities"]
                .as_array()
                .is_some_and(|caps| caps.iter().any(|cap| cap == "tool-result/v1"));
        self.0.insert((channel, epoch), v1);
        Ok(())
    }

    pub fn answer(&self, frame: &Frame, request: &Value) -> Result<Vec<u8>, String> {
        let body = match request
            .get("method")
            .or_else(|| request.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "role.describe" => {
                json!({"majors":[{"version":"tool-provider/v1","ops":["role.describe","tool.catalog"],"stability":"alpha"}],"implementation_version":"fixture","capabilities":[]})
            }
            "tool.catalog" => {
                let fixture: Value = serde_json::from_slice(
                    &std::fs::read(super::stand_in_value("SURFACE_FIXTURES"))
                        .map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
                let mut catalog = fixture["catalog"].clone();
                let tool = &mut catalog["tools"][0];
                if declares_v1() {
                    tool["result_format"] = json!("tool-result/v1");
                } else {
                    tool.as_object_mut()
                        .ok_or("fixture needs catalog.tools[0]")?
                        .remove("result_format");
                }
                catalog
            }
            "tools/list" => {
                json!({"tools":[{"name":"read","description":"Read a fixture","inputSchema":schema()}]})
            }
            "read" | "tools/call" => {
                let v1 = self
                    .0
                    .get(&(frame.header.channel, frame.header.epoch))
                    .copied()
                    .unwrap_or(false);
                let arguments = request
                    .get("arguments")
                    .unwrap_or(&request["params"]["arguments"]);
                reply(arguments["fixture"].as_str().unwrap_or("text"), v1)
            }
            method => return Err(format!("tool fixture does not serve {method}")),
        };
        serde_json::to_vec(&body).map_err(|error| error.to_string())
    }
}

fn reply(fixture: &str, v1: bool) -> Value {
    let (text, error) = match fixture {
        "image" => ("Read image attachment (image/png, 1×1, 69 B).", false),
        "error" => ("File not found: missing.txt", true),
        _ => ("Read text: 12:00\n", false),
    };
    let mut body = json!({"content":[{"type":"text","text":text}],"isError":error,
        "structuredContent":{"id":format!("read-{fixture}"),"success":!error,"text":text,"complete":true,"truncated":false}});
    if fixture == "image" {
        body["structuredContent"]["attachments"] = json!([{"kind":"image","mime":"image/png","data":PNG,"width":1,"height":1,"alt":"one pixel"}]);
        if v1 {
            body["content"].as_array_mut().unwrap().push(json!({"type":"image","mimeType":"image/png","data":PNG,"width":1,"height":1,"alt":"one pixel"}));
        }
    }
    if error {
        body["structuredContent"]["code"] = json!("NOT_FOUND");
        if v1 {
            body["errorCode"] = json!("not_found");
        }
    }
    body
}
