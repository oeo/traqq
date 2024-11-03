#![allow(dead_code)]

use rand::{thread_rng, Rng};
use rand::prelude::SliceRandom;
use rand::distributions::Alphanumeric;
use serde_json::json;

use crate::constants;

pub const SAMPLE_USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
];

pub const SAMPLE_UTM_SOURCES: &[&str] = &[
    "google",
    "facebook",
    "twitter",
    "linkedin",
    "instagram",
    "youtube",
    "pinterest",
    "tiktok",
    "snapchat",
    "reddit",
    "tumblr",
    "whatsapp",
    "telegram",
    "signal",
    "wechat",
];

pub const SAMPLE_UTM_MEDIUMS: &[&str] = &[
    "cpc",
    "organic",
    "referral",
    "email",
    "social",
    "display",
    "affiliate",
    "direct",
    "other",
];

pub const SAMPLE_UTM_TERMS: &[&str] = &[
    "loan",
    "credit",
    "insurance",
    "mortgage",
    "bank",
];

pub const SAMPLE_EVENTS: &[&str] = &[
    "page_view",
    "click",
    "purchase",
    "add_to_cart",
    "form_submission",
    "video_play",
    "video_pause",
    "video_stop",
    "video_complete",
    "video_ad_start",
    "video_ad_complete",
    "video_ad_skip",
    "video_ad_click",
    "video_ad_impression",
    "video_ad_error",
    "video_ad_pause",
    "video_ad_resume",
    "video_ad_progress",
    "video_ad_buffer",
    "video_ad_fullscreen",
    "video_ad_exit_fullscreen",
    "video_ad_mute",
    "video_ad_unmute",
    "video_ad_volume_change",
    "video_ad_duration_change",
    "video_ad_user_action",
    "video_ad_error",
    "video_ad_loaded",
    "video_ad_start",
    "video_ad_end",
    "video_ad_close",
    "video_ad_click",
    "video_ad_impression",
    "video_ad_error",
    "video_ad_pause",
    "video_ad_resume",
    "video_ad_progress",
    "video_ad_buffer",
    "video_ad_fullscreen",
    "video_ad_exit_fullscreen",
];

pub const SAMPLE_UTM_CAMPAIGNS: &[&str] = &[
    "campaign_1",
    "campaign_2",
    "campaign_3",
    "campaign_4",
    "campaign_5",
    "campaign_6",
    "campaign_7",
    "campaign_8",
    "campaign_9",
    "campaign_10",
    "summer_sale",
    "spring_sale",
    "back_to_school",
    "holiday_sale",
    "black_friday",
    "cyber_monday",
    "new_year",
    "valentines_day",
];

pub fn random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn create_test_event() -> serde_json::Value {
    let mut rng = thread_rng();
    let event = json!({
        "_id": rng.gen_range(111111111..999999999),
        "user_id": random_string(32),
        "session_id": random_string(32),
        "event": SAMPLE_EVENTS.choose(&mut rng).unwrap(),
        "url": format!("https://example.com/{}", random_string(4)),
        "utm_source": SAMPLE_UTM_SOURCES.choose(&mut rng).unwrap(),
        "utm_medium": SAMPLE_UTM_MEDIUMS.choose(&mut rng).unwrap(),
        "utm_term": SAMPLE_UTM_TERMS.choose(&mut rng).unwrap(),
        "user_agent": SAMPLE_USER_AGENTS.choose(&mut rng).unwrap(),
        "utm_campaign": SAMPLE_UTM_CAMPAIGNS.choose(&mut rng).unwrap(),
        "ip": generate_random_ip(),
        "browser": "Safari",
        "os": "macOS",
        "os_version": "11.5.2",
        "amount": (match rng.gen_range(0..4) {
            0 => json!(99.9),
            1 => json!(199.9), 
            2 => json!("0.99"),
            _ => json!(rng.gen_range(1..100))
        }),
        "device": {
            "type": "desktop",
            "brand": "Apple",
            "model": "MacBook Pro",
            "os": "macOS",
            "os_version": "11.5.2",
            "browser": "Safari",
            "browser_version": "14.1.2",
        },
        "location": {
            "city": "New York",
            "region": "NY",
            "country": "US",
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
    });

    event
}

pub fn generate_random_ip() -> String {
    let mut rng = thread_rng();
    format!("{}.{}.{}.{}",
        rng.gen_range(1..255),
        rng.gen_range(1..255),
        rng.gen_range(1..255),
        rng.gen_range(1..255)
    )
}

pub fn sort_keys(keys: &[String]) -> Vec<String> {
    let mut sorted = keys.to_vec();
    sorted.sort();
    sorted
}

pub fn sanitize_value(value: &str, max_length: usize) -> Result<Option<String>, String> {
    let max_length = if max_length == 0 { constants::MAX_VALUE_LENGTH } else { max_length };
    let mut result = value.trim().to_string();
    
    if result.is_empty() {
        return Ok(None);
    }

    result = result
        .chars()
        .take(max_length)
        .collect::<String>()
        .trim_matches(|c| constants::INVALID_CHARS.contains(&c))
        .to_string();

    for &invalid_char in &constants::INVALID_CHARS {
        result = result.replace(invalid_char, "_");
    }
    
    Ok((!result.is_empty()).then_some(result))
}

pub fn parse_timezone(tz: &str) -> Result<chrono_tz::Tz, String> {
    tz.parse().map_err(|_| format!("invalid timezone: {}", tz))
}

// validate a string mapping pattern 
pub fn validate_mapping_pattern(pattern: &str) -> Result<(), String> {
    let validation_rules = [
        (pattern.is_empty(), "pattern cannot be empty"),
        (pattern.contains("~~"), "pattern cannot contain consecutive separators"),
        (pattern.starts_with('~') || pattern.ends_with('~'), "pattern cannot start or end with separator"),
    ];

    validation_rules
        .iter()
        .find(|(condition, _)| *condition)
        .map_or(Ok(()), |(_, message)| Err(message.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_event() {
        let event = create_test_event();
        println!("{}", event);

        assert!(event.get("event").unwrap().is_string());
        assert!(event.get("_id").unwrap().is_number());
    }

    #[test]
    fn test_sanitize_value() {
        let value = "~hello~world~";
        let sanitized = sanitize_value(value, 0).unwrap().unwrap();
        assert_eq!(sanitized, "hello_world");
    }

    #[test]
    fn test_parse_timezone() {
        let tz = parse_timezone("America/New_York").unwrap();
        assert_eq!(tz, chrono_tz::Tz::America__New_York);
    }

    #[test]
    fn test_validate_mapping_pattern() {
        let pattern = "~event~";
        let result = validate_mapping_pattern(pattern);
        assert_eq!(result.is_err(), true);

        let pattern = "event";
        let result = validate_mapping_pattern(pattern);
        assert_eq!(result.is_ok(), true);

        let pattern = "event~source";
        let result = validate_mapping_pattern(pattern);
        assert_eq!(result.is_ok(), true);
    }

}

