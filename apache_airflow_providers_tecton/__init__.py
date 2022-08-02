def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-tecton",
        "name": "Apache Airflow Providers Tecton",
        "description": "Apache Airflow Providers for Tecton.",
        "hook-class-names": ["sample_provider.hooks.sample_hook.SampleHook"],
        "extra-links": ["sample_provider.operators.sample_operator.ExtraLink"],
        "versions": ["0.0.1"] # Required
    }
