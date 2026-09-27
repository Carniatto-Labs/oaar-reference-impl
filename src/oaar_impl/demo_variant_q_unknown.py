from oaar_impl import demo_abc_checkpoint

CONTEXT_PATH = "/tmp/oaar-abc-unknown-context.json"


def main() -> int:
    return demo_abc_checkpoint.main(q_outcome="unknown", context_path=CONTEXT_PATH)


if __name__ == "__main__":
    raise SystemExit(main())
