#Migration Tool

##Description
This tool was created for batch migration of projects. The source code of the tool can be found here: https://github.com/adriantoman/migration-tool.

##Currently supported steps are:

* Create export token under export login
* Create new project under main login
* Apply export token on new project
* Apply MAQL on new project
* Apply export metadata token on new project
* Set Zendeks integration on project

The tools is working on step by step basis. After each step is completed for all project, it will continue forward.

The tool is tested under ruby version 1.9.3, so you need to have this version of ruby installed on your computer (Tool should work also under other versions of ruby, but they are not tested). You also need rubygems tool installed. When you have all required prerequisites, run following set if commands:

##Instalation
```bash
git clone https://github.com/adriantoman/migration-tool.git migration-tool
cd migration-tool
gem install bundler
bundle install --path gems
```
##Configuration
The tool configuration is done by one JSON file. The config file (config.json) need to be in config folder (config/config.json). You need to also provide the project.csv file,
where are the PIDs for projects to migrate.

###Setting
In the connection section you need to specify credentials for Gooddata servers. The possible settings are:

* **export_login** (required) - GoodData login which is used for Export Token creation
* **export_password** (require) - Gooddata password
* export_server - GoodData server adress (default https://secure.gooddata.com)
* export_webdav - GoodData webdav adress (default https://secure-di.gooddata.com)
* **login** (required) - GoodData login (must be domain admin, if you want to use USER provisioning functionality)
* **password** (require) - Gooddata password
* server - GoodData server adress (default https://secure.gooddata.com)
* webdav - GoodData webdav adress (default https://secure-di.gooddata.com)


Example:
```json
    {
        "connection":{
            "export_login":"export_login",
            "export_password":"export_password",
            "export_server":"https://na1.gooddata.com",
            "export_webdav":"https://na1-di.gooddata.com",
            "login":"main_login",
            "password":"main_password",
            "server":"https://na1.gooddata.com",
            "webdav":"https://na1-di.gooddata.com"

        },
        "settings":{
            "token":"TOKEN_FOR_PROJECT_CREATION",
            "number_simultanious_projects":3,
            "maql_file":"source/maql.txt",
            "import_token":"IMPORT_METADATA_TOKEN"
         }
    }
```


##Execution
The execution part of the tools is quite strait forward. After you have successfully configured the application you can run it.


###Run
The dry run will show you what will be done in standard run. You can executed it by following command executed in folder where you have installed provisioning tool:

```bash
ruby bin/migration start
```

##FAQ



