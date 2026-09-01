use std::borrow::Cow;

pub fn escape_control(value: &str) -> Cow<'_, str> {
    if !value.chars().any(char::is_control) {
        return Cow::Borrowed(value);
    }
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    Cow::Owned(escaped)
}

#[cfg(test)]
mod tests {
    use super::escape_control;

    #[test]
    fn preserves_printable_text_and_escapes_controls() {
        assert_eq!(escape_control("name\n\u{1b}[31m"), "name\\n\\u{1b}[31m");
        assert_eq!(escape_control("cafe"), "cafe");
    }
}
