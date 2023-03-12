# Piperider profiler

Install Piperider:

```
pip install 'piperider[bigquery]'
```

Initialize it:

```
piperider init
```

To check the installation, run `piperider diagnose`, it should pass all tests.

Now build the dbt project and run Piperider:

```
dbt build
piperider run
```

At the end you should get a report (in html and json) that you can analyse.
