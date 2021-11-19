# Get the concurrency group for the running workflow

This action is intended for getting the concurrency group for the running 
workflow. This group name can be used with the `concurrency` feature to cancel 
outdated workflow runs. See for more [details](https://git.io/J1XHy).

## How to use

Add the following code to your workflow steps:

```
  - uses: actions/checkout@v2
  - id: concurrency
    uses: ./.github/actions/get-concurrency-group
```

Note, the checkout step must be done before.
