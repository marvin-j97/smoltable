// Define the allowed characters
const ALLOWED_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_#$";

pub fn is_valid_identifier(s: &str) -> bool {
    // Check if all characters in the string are allowed
    let all_allowed = s.chars().all(|c| ALLOWED_CHARS.contains(c));

    !s.is_empty() && s.len() < 512 && all_allowed
}
