# Ethics and Privacy

This project uses public Amazon Reviews 2023 data for educational analytics.

Guidelines:

- Do not attempt to identify individual reviewers.
- Treat `user_id` as a pseudonymous identifier.
- Do not publish raw full data files in the repository.
- Keep only small samples in GitHub.
- Report aggregate trends instead of individual user behavior.
- Document any filtering, quarantine, and deduplication logic.

The repository excludes `data/raw/` and generated outputs to avoid committing
large or sensitive derived artifacts.
