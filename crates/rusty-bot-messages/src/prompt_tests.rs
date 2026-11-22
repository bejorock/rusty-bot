use super::*;
fn vars(pairs: &[(&str, &str)]) -> PromptVariables {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
#[test]
fn strict_variables_unicode_and_escaped_braces() {
    let t = PromptTemplate::new("Hello {name}! {{example}} {name} 🦀").unwrap();
    assert_eq!(
        t.required_variables()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec!["name"]
    );
    assert_eq!(
        t.render(&vars(&[("name", "Rana")])).unwrap(),
        "Hello Rana! {example} Rana 🦀"
    );
    assert!(t.render(&vars(&[])).is_err());
    assert!(t.render(&vars(&[("name", "Rana"), ("typo", "x")])).is_err());
    for malformed in [
        "{",
        "}",
        "{a.b}",
        "{a b}",
        "{}",
        "{2name}",
        "{name!r}",
        "{name:>10}",
    ] {
        assert!(PromptTemplate::new(malformed).is_err(), "{malformed}");
    }
}
#[test]
fn variations_are_immutable_can_override_and_never_recursively_expand() {
    let base = PromptTemplate::new("Act as {role}. Reply in {language}: {input}").unwrap();
    let coding = base
        .partial(vars(&[
            ("role", "coding assistant"),
            ("language", "English"),
        ]))
        .unwrap();
    let indonesian = coding.partial(vars(&[("language", "Indonesian")])).unwrap();
    assert_eq!(base.required_variables().unwrap().len(), 3);
    assert_eq!(coding.required_variables().unwrap().len(), 1);
    assert_eq!(
        indonesian
            .render(&vars(&[("input", "{language}")]))
            .unwrap(),
        "Act as coding assistant. Reply in Indonesian: {language}"
    );
    assert_eq!(
        coding
            .render(&vars(&[("input", "hi"), ("language", "French")]))
            .unwrap(),
        "Act as coding assistant. Reply in French: hi"
    );
    assert!(base.partial(vars(&[("unknown", "x")])).is_err());
}
#[test]
fn serialized_templates_are_revalidated_on_use() {
    let t = PromptTemplate::new("{name}")
        .unwrap()
        .partial(vars(&[("name", "Rana")]))
        .unwrap();
    let json = serde_json::to_value(&t).unwrap();
    assert_eq!(
        serde_json::from_value::<PromptTemplate>(json)
            .unwrap()
            .render(&vars(&[]))
            .unwrap(),
        "Rana"
    );
    let invalid: PromptTemplate = serde_json::from_value(
        serde_json::json!({"source":"{valid}","bindings":{"typo":"hidden"}}),
    )
    .unwrap();
    assert!(invalid.render(&vars(&[("valid", "x")])).is_err());
    let invalid: PromptTemplate =
        serde_json::from_value(serde_json::json!({"source":"{oops","bindings":{}})).unwrap();
    assert!(invalid.render(&vars(&[])).is_err());
}
#[test]
fn chat_templates_scope_variables_and_preserve_multimodal_literals() {
    let image = ConversationMessage::User {
        content: vec![
            ContentBlock::text("literal {do_not_expand}"),
            ContentBlock::Image {
                source: MediaSource::Reference {
                    mime_type: "image/png".into(),
                    key: "upload".into(),
                },
            },
        ],
    };
    let t = ChatPromptTemplate {
        messages: vec![
            PromptMessage::Text {
                role: PromptRole::System,
                template: PromptTemplate::new("You are {role}")
                    .unwrap()
                    .partial(vars(&[("role", "helpful")]))
                    .unwrap(),
            },
            PromptMessage::Literal {
                message: image.clone(),
            },
            PromptMessage::Text {
                role: PromptRole::User,
                template: PromptTemplate::new("{question}").unwrap(),
            },
            PromptMessage::Text {
                role: PromptRole::Assistant,
                template: PromptTemplate::new("Acknowledged").unwrap(),
            },
        ],
    };
    let messages = t.render(&vars(&[("question", "Describe it")])).unwrap();
    assert_eq!(messages[0], ConversationMessage::system("You are helpful"));
    assert_eq!(messages[1], image);
    assert_eq!(messages[2], ConversationMessage::user("Describe it"));
    assert_eq!(messages[3], ConversationMessage::assistant("Acknowledged"));
    assert!(t.render(&vars(&[])).is_err());
    assert!(t
        .render(&vars(&[("question", "x"), ("extra", "y")]))
        .is_err());
    let restored: ChatPromptTemplate =
        serde_json::from_value(serde_json::to_value(&t).unwrap()).unwrap();
    assert_eq!(restored, t);
}
