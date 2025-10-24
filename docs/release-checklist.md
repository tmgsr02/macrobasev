# MacroBase Python Utilities Release Checklist

This checklist covers the steps required to publish a new version of the
``macrobasev`` Python distribution to PyPI.

1. **Preparation**
   - Make sure all pending work is merged and the codebase is clean.
   - Update ``CHANGELOG.md`` with the highlights for the release.
   - Confirm the version number you plan to release.

2. **Run tests and quality checks**
   - Execute the project's automated tests (if applicable).
   - Run ``python -m build`` to ensure sdist and wheel artifacts can be
     created successfully.
   - Run ``twine check dist/*`` to validate the distribution metadata.

3. **Bump the version**
   - Use ``bump2version`` to increment the version appropriately, for example:
     ``bump2version patch``.
   - Push the resulting commit and tag to the upstream repository.

4. **Publish to PyPI**
   - Ensure you have valid credentials configured for ``twine``.
   - Upload the artifacts with ``twine upload dist/*``.
   - Verify that the release appears on https://pypi.org/project/macrobasev/.

5. **Post-release**
   - Announce the release as appropriate (mailing list, Slack, etc.).
   - Create an issue or milestone for the next development cycle.

Following these steps ensures that each release is consistent, well-documented,
and reproducible.
