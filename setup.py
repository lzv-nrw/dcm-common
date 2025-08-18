from setuptools import setup


setup(
    version="3.26.7",
    name="dcm-common",
    description=(
        "common functions and components for the Digital Curation Manager"
    ),
    author="LZV.nrw",
    license="MIT",
    python_requires=">=3.10",
    install_requires=[],
    extras_require={
        "services": [
            "flask>=3",
            "requests>=2",
            "pytest>=7",
            "data-plumber-http>=0.3.0,<2",
        ],
        "db": [
            "flask>=3",
            "requests>=2",
            "psycopg[binary]>=3",
        ],
        "orchestration": [
            "flask>=3",
            "requests>=2",
        ],
        "xml": [
            "xmlschema>=3.3,<4",
        ],
    },
    packages=[
        "dcm_common",
        "dcm_common.db",
        "dcm_common.db.key_value_store",
        "dcm_common.db.key_value_store.adapter",
        "dcm_common.db.key_value_store.backend",
        "dcm_common.db.key_value_store.middleware",
        "dcm_common.db.key_value_store.middleware.flask",
        "dcm_common.db.sql",
        "dcm_common.db.sql.adapter",
        "dcm_common.models",
        "dcm_common.orchestration",
        "dcm_common.plugins",
        "dcm_common.services",
        "dcm_common.services.adapter",
        "dcm_common.services.demo",
        "dcm_common.services.extensions",
        "dcm_common.services.notification",
        "dcm_common.services.tests",
        "dcm_common.services.views",
    ],
    package_data={
        "dcm_common": ["py.typed"],
        "dcm_common.db.key_value_store.middleware.flask": [
            "openapi.yaml",
        ],
    },
    setuptools_git_versioning={
        "enabled": True,
        "version_file": "VERSION",
        "count_commits_from_version_file": True,
        "dev_template": "{tag}.dev{ccount}",
    },
)
