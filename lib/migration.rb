require "yaml"
require "helper"
%w(storage object).each {|a| require "persistent/#{a}"}


module Migration


  class MigrationTasks

    def initialize
      fail "The config file don't exist" if !File.exist?("config/config.json")
      File.open( "config/config.json", "r" ) do |f|
        json = JSON.load( f )
        @connection_username = json["connection"]["login"]
        @connection_password = json["connection"]["password"]
        @connection_server = json["connection"]["server"] || "https://secure.gooddata.com"
        @connection_webdav = json["connection"]["webdav"] || "https://secure-di.gooddata.com"
        @connection_token = json["connection"]["token"]
        @connection_export_username = json["connection"]["export_login"]
        @connection_export_password = json["connection"]["export_password"]
        @connection_export_server = json["connection"]["export_server"] || "https://secure.gooddata.com"
        @connection_export_webdav = json["connection"]["export_webdav"] || "https://secure-di.gooddata.com"
        @connection_zendesk_username = json["connection"]["zendesk_username"]
        @connection_zendesk_password = json["connection"]["zendesk_password"]


        @settings_token = json["settings"]["token"]
        @settings_number_simultanious_projects = json["settings"]["number_simultanious_projects"]
        @settings_maql_file = json["settings"]["maql_file"]
        @settings_import_token = json["settings"]["import_token"]
        @settings_project_file = json["settings"]["project_file"]
        @settings_project_template = json["settings"]["project_template"]
        @settings_color_palete = json["settings"]["color_palete"] || nil
        @settings_upload_files = json["settings"]["upload_files"]
      end
      GoodData.logger = $log
      GoodData.logger.level = Logger::DEBUG
      #GoodData.connect(@connection_username,@connection_password,{:webdav_server => @connection_webdav,:server => @connection_server})
      #GoodData.connect(@connection_username,@connection_password,@connection_server,{:webdav_server => @connection_webdav})
      Storage.load_data
      Storage.store_data
    end




    def connect_for_export()
      pp "connecting"
      GoodData.connect(@connection_export_username,@connection_export_password,{:webdav_server => @connection_export_webdav,:server => @connection_export_server})
    end


    def connect_for_work()
      GoodData.connect(@connection_username,@connection_password,{:webdav_server => @connection_webdav,:server => @connection_server})
    end

    def disconnect()
      GoodData.disconnect
    end


    def load_source_data
      fail "Project file don't exists" if !File.exists?(@settings_project_file)
      pids = []
      CSV.foreach(@settings_project_file, {:headers => true, :skip_blanks => true}) do |csv_obj|
        pids.push({"pid" => csv_obj["project_pid"], "type" => csv_obj["type"]})
      end
      pids.uniq! {|p| p["pid"]}
      pids.each do |hash|
        object = Storage.get_object_by_old_project_pid(hash["pid"])
        if (object.nil?)
          object = Object.new()
          object.old_project_pid = hash["pid"]
          fail "The project type |#{hash["type"]}| for pid #{hash["pid"]} is not valid. Valid types: #{Object.VALID_TYPES.join(",")}" if Object.VALID_TYPES.find{|t| t == hash["type"]}.nil?
          object.type = hash["type"]
          object.status = Object.NEW
          Storage.add_object(object)
        end
      end
    end


    def load_data
      connect_for_export()
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          project = GoodData.get("/gdc/projects/#{object.old_project_pid}")
          object.title = project["project"]["meta"]["title"]
          object.summary = project["project"]["meta"]["summary"]
          if (object.type == "migration")
            object.status = Object.NEW
          elsif (object.type == "template")
            object.status = Object.CLONED
          end
          Storage.store_data
        end
      end
    end

    def get_export_tokens_projects
      connect_for_export()
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          $log.info "Getting export token for project: #{object.old_project_pid} (new pid #{object.new_project_pid})"

          export = {
              :exportProject => {
                  :exportUsers => 0,
                  :exportData => 0,
                  :authorizedUsers => [@connection_username]
              }
          }

          result = GoodData.post("/gdc/md/#{object.old_project_pid}/maintenance/export", export)
          object.export_token = result['exportArtifact']['token']
          object.export_status_url = result['exportArtifact']['status']['uri']
          object.status = Object.CLONE_REQUESTED
          Storage.store_data
        end

        while (Storage.get_objects_by_status(Object.CLONE_REQUESTED).count >= @settings_number_simultanious_projects)
          $log.info "Waiting till all export token are generated"
          Storage.get_objects_by_status(Object.CLONE_REQUESTED).each do |for_check|
            state = GoodData.get(for_check.export_status_url)['taskState']['status']
            if (state == 'OK')
              $log.info "Token for #{for_check.old_project_pid} successfully created"
              for_check.status = Object.CLONED
              Storage.store_data
            elsif  (state == "ERROR")
              for_check.status = Object.NEW
              Storage.store_data
              $log.error "Generating export token for pid #{for_check.old_project_pid} has failed - please restart"
            end
          end

          if (Storage.get_objects_by_status(Object.CLONE_REQUESTED).count >= @settings_number_simultanious_projects)
            $log.info "Waiting - START"
            sleep(10)
            $log.info "Waiting - STOP"
          end
        end
      end


      while (Storage.get_objects_by_status(Object.CLONE_REQUESTED).count > 0)
        $log.info "Waiting till all export token are generated"
        Storage.get_objects_by_status(Object.CLONE_REQUESTED).each do |for_check|
          state = GoodData.get(for_check.export_status_url)['taskState']['status']
          if (state == 'OK')
            $log.info "Token for #{for_check.old_project_pid} successfully created"
            for_check.status = Object.CLONED
            Storage.store_data
          elsif  (state == "ERROR")
            for_check.status = Object.NEW
            Storage.store_data
            $log.error "Generating export token for pid #{for_check.old_project_pid} has failed"
          end
        end
        if (Storage.get_objects_by_status(Object.CLONE_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
      disconnect
    end




    def create_projects
      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.CLONED)
          $log.info "Creating clone object for project #{object.old_project_pid}"

          json =
              {
                  'project' => {
                      'meta' => {
                          'title' => object.title + " v4",
                          'summary' => object.summary
                      },
                      'content' => {
                          'guidedNavigation' => 1,
                          'driver' => 'Pg',
                          'authorizationToken' => @settings_token
                      }
                  }
              }

          if (object.type == "template")
            json["project"]["meta"].merge! ({ "projectTemplate" => @settings_project_template })
          end

          project = GoodData::Project.new json
          project.save
          $log.info "New project created under pid #{project.obj_id}"
          object.status = Object.PROVISIONING
          object.new_project_pid = project.obj_id
          Storage.store_data
        end

        while (Storage.get_objects_by_status(Object.PROVISIONING).count >= @settings_number_simultanious_projects)
          $log.info "Waiting till all created project are provisioned"
          Storage.get_objects_by_status(Object.PROVISIONING).each do |for_check|
            project_status = GoodData::Project[for_check.new_project_pid]
            #pp project_status.to_json["project"]['content']['state']
            if (project_status.state == :enabled)
              $log.info "Project #{for_check.new_project_pid} successfully provisioned"
              for_check.status = Object.CREATED
              Storage.store_data
            elsif (project_status.state == :deleted or project_status.state == :archived)
              $log.error "Project #{for_check.new_project_pid} was not provisioned"
              for_check.status = Object.CLONED
              Storage.store_data
            end
          end

          if (Storage.get_objects_by_status(Object.PROVISIONING).count >= @settings_number_simultanious_projects)
            $log.info "Waiting - START"
            sleep(10)
            $log.info "Waiting - STOP"
          end
        end
      end


      while (Storage.get_objects_by_status(Object.PROVISIONING).count > 0)
        $log.info "Waiting till all created project are provisioned"
        Storage.get_objects_by_status(Object.PROVISIONING).each do |for_check|
          project_status = GoodData::Project[for_check.new_project_pid]
          if (project_status.state == :enabled)
            $log.info "Project #{for_check.new_project_pid} successfully provisioned"
            for_check.status = Object.CREATED
            Storage.store_data
          elsif (project_status.state == :deleted or project_status.state == :archived)
            $log.error "Project #{for_check.new_project_pid} was not provisioned"
            for_check.status = Object.CLONED
            Storage.store_data
          end
        end

        if (Storage.get_objects_by_status(Object.PROVISIONING).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end

    end




    def import_projects
      Storage.object_collection.each do |object|
        if (object.status == Object.CREATED and object.type == "migration")
          $log.info "Starting import for project: #{object.old_project_pid} (new pid #{object.new_project_pid}"

          import = {
              :importProject => {
                  :token => object.export_token
              }
          }
          result = GoodData.post("/gdc/md/#{object.new_project_pid}/maintenance/import", import)
          object.import_status_url = result['uri']
          object.status = Object.IMPORT_REQUESTED
          Storage.store_data
        elsif (object.status == Object.CREATED and object.type == "template")
          #Lets fake that the project was imported, because in case of template we are not importing
          #Moving directly after the Parial metada import export, because none of this task is done for template projects
          object.status = Object.PARTIAL
          Storage.store_data
        end

        while (Storage.get_objects_by_status(Object.IMPORT_REQUESTED).count >= @settings_number_simultanious_projects)
          $log.info "Waiting till all project tokens are imported"
          Storage.get_objects_by_status(Object.IMPORT_REQUESTED).each do |for_check|
            state = GoodData.get(for_check.import_status_url)['taskState']['status']
            if (state == 'OK')
              $log.info "Token for #{for_check.new_project_pid} successfully created"
              for_check.status = Object.IMPORTED
              Storage.store_data
            elsif  (state == "ERROR")
              for_check.status = Object.CREATED
              Storage.store_data
              $log.error "Importing token for pid #{for_check.new_project_pid} has failed - please restart"
            end
          end

          if (Storage.get_objects_by_status(Object.IMPORT_REQUESTED).count >= @settings_number_simultanious_projects)
            $log.info "Waiting - START"
            sleep(10)
            $log.info "Waiting - STOP"
          end
        end
      end





      while (Storage.get_objects_by_status(Object.IMPORT_REQUESTED).count > 0)
        $log.info "Waiting till all project tokens are imported"
        Storage.get_objects_by_status(Object.IMPORT_REQUESTED).each do |for_check|
          state = GoodData.get(for_check.import_status_url)['taskState']['status']
          if (state == 'OK')
            $log.info "Token for #{for_check.new_project_pid} successfully created"
            for_check.status = Object.IMPORTED
            Storage.store_data
          elsif  (state == "ERROR")
            for_check.status = Object.CREATED
            Storage.store_data
            $log.error "Importing token for pid #{for_check.new_project_pid} has failed - please restart"
          end
        end
        if (Storage.get_objects_by_status(Object.IMPORT_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
    end

    def execute_maql

      fail "Cannot find MAQL file" if !File.exist?(@settings_maql_file)
      maql_source = File.read(@settings_maql_file)
      Storage.object_collection.each do |object|
        if (object.status == Object.IMPORTED)
          $log.info "Starting maql execution on : #{object.new_project_pid}"
          maql = {
              "manage" => {
                  "maql" => maql_source
              }

          }
          begin
            result = GoodData.post("/gdc/md/#{object.new_project_pid}/ldm/manage2", maql)
            task_id = result["entries"].first["link"].match(/.*\/tasks\/(.*)\/status/)[1]
            object.maql_update_task_id = task_id
            object.status = Object.MAQL_REQUESTED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            @@log.warn "The maql could not be applied on project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            @@log.warn "The maql could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            @@log.warn "Unknown error - The maql could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          end

          while (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count >= @settings_number_simultanious_projects)
            $log.info "Waiting till all MAQL is applied on all projects"
            Storage.get_objects_by_status(Object.MAQL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.new_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                $log.info "MAQL for project #{for_check.new_project_pid} successfully applied"
                for_check.status = Object.MAQL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.IMPORTED
                Storage.store_data
                $log.error "Applying MAQL on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
              end
            end

            if (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count >= @settings_number_simultanious_projects)
              $log.info "Waiting - START"
              sleep(10)
              $log.info "Waiting - STOP"
            end
          end
        end
      end

      while (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count > 0)
        $log.info "Waiting till all MAQL is applied on all projects"
        Storage.get_objects_by_status(Object.MAQL_REQUESTED).each do |for_check|
          result = GoodData.get("/gdc/md/#{for_check.new_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            $log.info "MAQL for project #{for_check.new_project_pid} successfully applied"
            for_check.status = Object.MAQL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.IMPORTED
            Storage.store_data
            $log.error "Applying MAQL on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
    end

    def execute_partial
      fail "The partial metada import token is empty" if @settings_import_token.nil? or @settings_import_token == ""
      Storage.object_collection.each do |object|
        if (object.status == Object.MAQL)
          json = {
              "partialMDImport" => {
                  "token" => "#{@settings_import_token}",
                  "overwriteNewer" => "1",
                  "updateLDMObjects" => "0"
              }
          }
          begin
            result = GoodData.post("/gdc/md/#{object.new_project_pid}/maintenance/partialmdimport", json)
            task_id = result["uri"].match(/.*\/tasks\/(.*)\/status/)[1]
            object.partial_metadata_task_id = task_id
            object.status = Object.PARTIAL_REQUESTED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The partial metadata could not be applied on project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"

          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The partial metadata could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The partial metadata could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["message"]}"
          end


          while (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > @settings_number_simultanious_projects)
            $log.info "Waiting till all Partial Metadata is applied on all projects"
            Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.new_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                $log.info "Partial Metadata for project #{for_check.new_project_pid} successfully applied"
                for_check.status = Object.PARTIAL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.MAQL
                Storage.store_data
                $log.error "Applying Partial Metadata on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
              end
            end

            if (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > @settings_number_simultanious_projects)
              $log.info "Waiting - START"
              sleep(10)
              $log.info "Waiting - STOP"
            end
          end
        end
      end
      while (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > 0)
        $log.info "Waiting till all Partial Metadata is applied on all projects"
        Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).each do |for_check|
          result = GoodData.get("/gdc/md/#{for_check.new_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            $log.info "Partial Metadata for project #{for_check.new_project_pid} successfully applied"
            for_check.status = Object.PARTIAL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.MAQL
            Storage.store_data
            $log.error "Applying Partial Metadata on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
    end



    def create_integration
      Storage.object_collection.each do |object|
        if (object.status == Object.PARTIAL)

          json = {
              "integration" => {
                  "projectTemplate" => "/projectTemplates/ZendeskAnalytics/10",
                  "active" => true
              }
          }
          begin
            result = GoodData.post("/gdc/projects/#{object.new_project_pid}/connectors/zendesk4/integration", json)
            object.status = Object.INTEGRATION_CREATED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The zendesk integration for project #{object.new_project_pid} could not be created. Reason: #{response["error"]["message"]}"

          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The zendesk integration for project #{object.new_project_pid} could not be created. Rturned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The zendesk integration for project #{object.new_project_pid} could not be created and returned 500. Reason: #{response["message"]}"
          end
        end
      end
    end


    def create_endpoint
      Storage.object_collection.each do |object|
        if (object.status == Object.INTEGRATION_CREATED)

          json = {
              "settings" => {
                 "apiUrl" => "https://gooddata.zendesk.com"
            }
          }
          begin
            result = GoodData.put("/gdc/projects/#{object.new_project_pid}/connectors/zendesk4/integration/settings", json)
            object.status = Object.ENDPOINT_SET
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The zendesk endpoint for project #{object.new_project_pid} could not be created. Reason: #{response["error"]["message"]}"

          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The zendesk endpoint for project #{object.new_project_pid} could not be created. Returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The zendesk endpoint for project #{object.new_project_pid} could not be created and returned 500."
          end
        end
      end
    end


    def run_integration
      Storage.object_collection.each do |object|
        if (object.status == Object.ENDPOINT_SET)
          json = {
              "process" => {}
          }
          begin
            result = GoodData.post("/gdc/projects/#{object.new_project_pid}/connectors/zendesk4/integration/processes", json)
            object.status = Object.ENDPOINT_SET_FINISHED
            object.zendesk_sync_process = result["uri"]
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The zendesk process for project #{object.new_project_pid} could not be started. Reason: #{response["error"]["message"]}"

          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The zendesk process for project #{object.new_project_pid} could not be started. Returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The zendesk process for project #{object.new_project_pid} could not be started and returned 500. Reason: #{response["message"]}"
          end
        end
      end
    end


    def apply_color_template
      Storage.object_collection.each do |object|
        if (object.status == Object.ENDPOINT_SET_FINISHED and !@settings_color_palete.nil?)
          begin
            result = GoodData.put("/gdc/projects/#{object.new_project_pid}/styleSettings", @settings_color_palete)
            object.status = Object.FINISHED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "Adding color palete to project #{object.new_project_pid} has failed. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "Adding color palete to project #{object.new_project_pid} has failed. Returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - Adding color palete to project #{object.new_project_pid} has failed and returned 500. Reason: #{response["message"]}"
          end
        elsif (object.status == Object.ENDPOINT_SET_FINISHED and @settings_color_palete.nil?)
          object.status = Object.FINISHED
          Storage.store_data
        end
      end
    end


    def conntact_zendesk_endpoint
      auth = {:username => @connection_zendesk_username, :password => @connection_zendesk_password}
      @subdomain = "?"
      response = HTTParty.get("https://#{@subdomain}.zendesk.com/api/v2/gooddata_integration/upgrade.json",:basic_auth => auth)
    end


    def print_status
      $log.info "----------------------------------------------------"
      $log.info "The final overview"
      Storage.object_collection.each do |object|
        $log.info "The project migration from #{object.old_project_pid} to #{object.new_project_pid} has finished at status #{object.status}"
        $log.info "The process uri is: #{object.zendesk_sync_process}"
      end
      $log.info "----------------------------------------------------"
    end



    def upload_file(continue = false)
      connect_for_work()
      if (!continue)
        # If we are not continuing, lets reset everything to beginning state
        Storage.object_collection.each do |object|
          object.uploads = []
          object.upload_finished = false
          @settings_upload_files.each do |file|
            object.uploads << {"name" => file.keys.first,"path" => file.values.first, "status" => Object.UPLOAD_NEW}
          end
        end
      end
      @settings_upload_files.each do |file|
        GoodData.connection.upload(file.values.first,{:directory => file.keys.first,:staging_url => @connection_webdav +  "/uploads/"})
      end

      while (Storage.object_collection.find_all{|o| o.upload_finished == false}.count > 0)
        Storage.object_collection.find_all{|o| o.upload_finished == false}.each do |object|
          running_task = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_RUNNING}
          if (running_task.nil?)
            new_upload = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_NEW}
            if (!new_upload.nil?)
              json = {
                      "pullIntegration" => "/#{new_upload["name"]}"
                     }
              begin
                res = GoodData.post("/gdc/md/#{object.new_project_pid}/etl/pull", json)
                new_upload["uri"] = res["pullTask"]["uri"]
                new_upload["status"] = Object.UPLOAD_RUNNING
                running_task = new_upload
              rescue RestClient::BadRequest => e
                response = JSON.load(e.response)
                new_upload["status"] = Object.UPLOAD_ERROR
                $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
              rescue RestClient::InternalServerError => e
                response = JSON.load(e.response)
                new_upload["status"] = Object.UPLOAD_ERROR
                $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
              rescue => e
                new_upload["status"] = Object.UPLOAD_ERROR
                $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.new_project_pid}.. Reason: Unknown reason"
              end
              Storage.store_data
            else
              object.upload_finished = true
              Storage.store_data
            end
          end
          if (!running_task.nil?)
            begin
              response = GoodData.get(running_task["uri"])
              if (response["taskStatus"] == "OK" || response["taskStatus"] == "WARNING")
                running_task["status"] = Object.UPLOAD_OK
              elsif (response["taskStatus"] == "ERROR")
                running_task["status"] = Object.UPLOAD_ERROR
              end
            rescue => e
              running_task["status"] = Object.UPLOAD_ERROR
              $log.warn "Upload of file #{running_task["name"]} has failed for project #{object.new_project_pid}. Reason: Unknown reason"
            end
            Storage.store_data
          end
        end
        sleep(5)
      end
    end










  end




end
