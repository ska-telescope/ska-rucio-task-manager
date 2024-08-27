# Correspondence

## 03/07/2023

If you want to follow along and write the example test you will need:
- git
- docker
- python3
- vim (or any other text editor for simple copy/paste)
- jq
- bash

In addition, you will need:

- An ESCAPE IAM account (for datalake auth) with membership to the escape/ska group
- An SKA IAM account (for monitoring auth) with membership to the monitoring/grafana/editor group (if wanting to play around with visualising output)

I can accept user/group requests for both the IAMs above, just let me know when you've requested it. It would be especially prudent to register an ESCAPE IAM account now if you want to participate as this can take a short while (~5min) to sync.

Obviously if you just want to watch that is fine also.

# Resources

Presentation: https://docs.google.com/presentation/d/1I8rRuxbUcjQTbvp2wipy7m7nS9Gagy5u2B0HEtsjg-w/edit#slide=id.g2562869c95b_1_149

# Post workshop notes

- Issues around Docker (make sure user is either in correct group or sudo is used anytime a docker command is issued in the setup_environment_for_skao script)
- Always `docker pull` the latest version of the client image before setting up the environment (host domains may have changed in the rucio-client)
- Building the image itself can take ~10min!

