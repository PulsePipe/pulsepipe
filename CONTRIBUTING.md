# Contributing to PulsePipe

Thank you for considering contributing to PulsePipe! We believe healthcare data infrastructure should be **open**, **high-quality**, and **community-driven**.

Your contributions — whether big or small — help improve healthcare data pipelines for everyone.

---

## 💡 Ways to Contribute

We welcome contributions of all types:

- New ingestion modules (custom HL7 segments, FHIR resources, etc.)
- Improvements to normalization and de-identification logic
- Embedding optimizations or integration of new embedding backends
- Bug reports and fixes
- Test cases and coverage improvements
- Documentation improvements
- Feature requests and architectural proposals

Please [open an issue](https://github.com/PulsePipe/pulsepipe/issues) if you have ideas, suggestions, or would like feedback before you start.

---

## 🛠 Development Setup

### Fork & Clone

```bash
git clone https://github.com/<your-username>/pulsepipe.git
cd pulsepipe
```

### Install Dependencies

```bash
poetry install
```

### (Optional) Enter Virtual Environment

```bash
poetry shell
```

### Run Tests

```bash
poetry run pytest
```

---

## ✅ Contribution Guidelines

- Keep pull requests focused and scoped.
- Add tests for new features or fixes.
- Use `black` and `isort` to automatically format your code:

```bash
poetry run black src/ tests/
poetry run isort src/ tests/
```

- Follow PEP8 and existing coding conventions.
- Use type hints when applicable.
- Be careful when handling clinical data, even in test scenarios.

---

## ⚖ License Agreement

By contributing to PulsePipe, you agree that:

- Your contributions will be released under the [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.html).
- Any modifications, when distributed or deployed as part of a network service, must be made available to the community under the same license.

This ensures that PulsePipe remains an open and trusted tool for the healthcare community.

---

Thank you for helping improve PulsePipe!
