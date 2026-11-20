use rusty_bot_core::{CommandSpec, ExecOptions, ExecutionErrorCode, Shell};
use rusty_bot_local::LocalShell;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn unix_shell_executes_and_still_honors_cancellation() {
    let shell = LocalShell::new("test", std::env::current_dir().unwrap());
    let output = shell
        .exec(
            CommandSpec::Shell {
                command: "printf clean".into(),
            },
            ExecOptions::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout, "clean");
    let cancel = CancellationToken::new();
    cancel.cancel();
    let error = shell
        .exec(
            CommandSpec::Shell {
                command: "exit 99".into(),
            },
            ExecOptions::default(),
            cancel,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ExecutionErrorCode::Aborted);
    let timeout = ExecOptions {
        timeout: Some(std::time::Duration::from_millis(50)),
        ..Default::default()
    };
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        shell.exec(
            CommandSpec::Shell {
                command: "sleep 30".into(),
            },
            timeout,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("timeout must terminate the running process group")
    .unwrap_err();
    assert_eq!(error.code, ExecutionErrorCode::Timeout);
    shell.cleanup().await.unwrap();
}
