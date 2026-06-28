# prefect-managed-spike

Throwaway spike. One long-running Prefect flow (`flow.py:long_job`) used to prove
that a Prefect **Managed** work pool runs a long job to completion on detached
infrastructure — the zero-to-one validation for moving DPYC's `start_async_job`
durable executor onto `run_deployment`. Safe to delete once the test passes.
