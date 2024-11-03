use traqq::prelude::*;

fn main() {
    let config = TraqqConfig {
        time: TimeConfig::default(),
        mapping: MappingConfig {
            bitmap: vec!["ip".to_string()],
            add: vec![
                "event".to_string(),
                "event~utm_campaign".to_string(),
                "event~utm_source~utm_medium".to_string(),
                "event~utm_source~utm_medium~utm_campaign".to_string(),
                "event~os".to_string(),
            ],
            add_value: vec![
                AddValueConfig { key: "event".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_campaign".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_source~utm_medium".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~utm_source~utm_medium~utm_campaign".to_string(), add_key: "amount".to_string() },
                AddValueConfig { key: "event~os".to_string(), add_key: "amount".to_string() },
            ],
        },
        limits: LimitsConfig::default(),
    };

    let json_event = traqq::utils::create_test_event();
    let event = IncomingEvent::from_json(json_event).unwrap();

    match ProcessedEvent::from_incoming(event, &config) {
        Ok(processed) => {
            processed.pretty_print();
        },
        Err(e) => {
            println!("Error processing event: {}", e);
        }
    }
}
