# Get concurrency options for the running workflow

This action is intended for getting concurrency options for the running 
workflow. These options can be used with the workflow concurrency feature. 
See for more [details](https://docs.github.com/en/actions/learn-github-actions/workflow-syntax-for-github-actions#concurrency).

## How to use

Add the following code to your workflow steps:

```
  - uses: actions/checkout@v2
  - id: concurrency
    uses: ./.github/actions/get-concurrency-opts
```

Note, the checkout step must be done before.
