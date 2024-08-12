## AusSRC tests

We have created a new test that does the following:

* Create a dummy fits file
* Upload fits file to RSE
* Add relevant metadata so that it is searchable via SKA SRCnet rucio datalake TAP service
* Perform TAP query to discover file
* Remove file from rucio datalake
* Cleanup
