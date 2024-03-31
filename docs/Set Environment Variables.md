## Set for the current session

```commandline
setx OPENAI_API_KEY "your-api-key-here"
```

## Set permanently

1. Right-click on `This PC` or `My Computer` and select `Properties`.
2. Click on `Advanced system settings`.
3. Click the `Environment Variables` button.
4. In the `System variables` section, click 'New...' and enter OPENAI_API_KEY as the variable name and your API key as the variable value.

## Verify

To verify the setup, reopen the command prompt and type the command below. It should display your API key: `echo %OPENAI_API_KEY%`

## Troubleshooting
You may need to adjust settings in your IDE so the project or script can access the variable. 

### Pycharm
Open the Run Configuration selector in the top-right (looks like a drop down box) and click `Edit Configurations`.

## Links
- https://platform.openai.com/docs/quickstart/step-2-set-up-your-api-key