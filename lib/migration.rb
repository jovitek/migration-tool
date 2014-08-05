require "yaml"
require "helper"
require "time"
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

        @settings_upload_files_sanitize = json["settings"]["upload_files_sanitize"]
        @settings_domain = json["settings"]["domain"]
        @settings_user_to_add = json["settings"]["user_to_add"]
        @settings_project_name_prefix = json["settings"]["project_name_prefix"]
        @settings_project_name_postfix = json["settings"]["project_name_postfix"]
        @settings_project_summary = json["settings"]["project_summary"]
        @settings_swap_config = json["settings"]["swap_config"]

      end
      GoodData.logger = $log
      GoodData.logger.level = Logger::DEBUG
      #GoodData.connect(@connection_username,@connection_password,{:webdav_server => @connection_webdav,:server => @connection_server})
      #GoodData.connect(@connection_username,@connection_password,@connection_server,{:webdav_server => @connection_webdav})
      Storage.load_data
      Storage.store_data
    end




    def connect_for_export()
      GoodData.connect(@connection_export_username,@connection_export_password,{:webdav_server => @connection_export_webdav,:server => @connection_export_server})
    end


    def connect_for_work()
      GoodData.connect(@connection_username,@connection_password,{:webdav_server => @connection_webdav,:server => @connection_server})
    end

    def disconnect()
      GoodData.disconnect
    end


    def load_source_data
      inf = Time.now.inspect  + " - loading source data from csv"
      puts(inf)
      $log.info inf

      fail "Project file don't exists" if !File.exists?(@settings_project_file)
      pids = []
      CSV.foreach(@settings_project_file, {:headers => true, :skip_blanks => true}) do |csv_obj|
        pids.push({"pid" => csv_obj["project_pid"], "type" => csv_obj["type"], "zd_account" => csv_obj["account"]})
      end
      pids.uniq! {|p| p["pid"]}
      pids.each do |hash|
        object = Storage.get_object_by_old_project_pid(hash["pid"])
        if (object.nil?)
          object = Object.new()

          object.old_project_pid = hash["pid"]
         
          fail "The project type |#{hash["type"]}| for pid #{hash["pid"]} is not valid. Valid types: #{Object.VALID_TYPES.join(",")}" if Object.VALID_TYPES.find{|t| t == hash["type"]}.nil?
          object.type = hash["type"]
          object.zd_account = hash["zd_account"]
          object.status = Object.NEW
          Storage.add_object(object)
        end
      end
    end


    def load_data
      inf = Time.now.inspect + " - fetching connector settings from Z3 projects"
      puts(inf)
      $log.info inf
      connect_for_export()
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          begin

            if object.zd_account && object.zd_account != "" && object.type == "template"
              object.api_url = "https://" +  object.zd_account + ".zendesk.com"
              object.title = object.zd_account
              object.summary = ''

            else 
              project = GoodData.get("/gdc/projects/#{object.old_project_pid}")
              object.title = project["project"]["meta"]["title"]
              object.summary = project["project"]["meta"]["summary"]
              integration_setting = GoodData.get("/gdc/projects/#{object.old_project_pid}/connectors/zendesk3/integration/config/settings")
              object.api_url = integration_setting["settings"]["apiUrl"]
            end

            if (object.type == "migration")
              object.status = Object.NEW
            elsif (object.type == "template")
              object.status = Object.CLONED
            end
            Storage.store_data
          rescue => e
            $log.warn "The project #{object.old_project_pid} is being ignored, because we cannot get source data"
            object.status = Object.IGNORE
            Storage.store_data
          end
        end
      end
    end

    def get_export_tokens_projects

      inf = Time.now.inspect  + " - exporting source projects for cloning"
       puts(inf)
      $log.info inf
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
      inf = Time.now.inspect  + " - creating target projects"
      puts(inf)
      $log.info inf
      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.CLONED)
          $log.info "Creating clone object for project #{object.old_project_pid}"

          json =
              {
                  'project' => {
                      'meta' => {
                          'title' => @settings_project_name_prefix + object.title + @settings_project_name_postfix,
                          'summary' => object.summary + @settings_project_summary
                      },
                      'content' => {
                          'guidedNavigation' => 1,
                          'driver' => 'Pg',
                          'authorizationToken' => @settings_token
                      }
                  }
              }

          if (object.type == "template")
            json["project"]["meta"].merge!({ "projectTemplate" => @settings_project_template })
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
      inf = Time.now.inspect  + " - importing clone tokens"
       puts(inf)
      $log.info inf
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
          object.status = Object.TAGGED
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



    def tag_entities
      inf = Time.now.inspect  + " - tagging metrics"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.IMPORTED)
          begin
            GoodData.with_project(object.new_project_pid) do |project|
              metrics = GoodData::Metric[:all].map { |meta|  GoodData::Metric[meta['link']]}
              metrics.map do |x|

                begin
                  x.tags =  x.tags + " migrated"
                  x.save
                rescue
                  $log.warn "Unable to tag metric: " + x.link
                end 

              end

              a = GoodData::Attribute['attr.zendesktickets.satisfactionscore']

              links = a.usedby('metric').map { |x| x['link'] }
              #pp links

              links.each { |x|
                
                begin
                  metric = GoodData::Metric[x]
                #  pp metric
                  metric.tags = metric.tags + " migrated_checkSat"
                  metric.save
                rescue
                  $log.warn "Unable to tag metric: " + x.link
                end 
              }
             

            end


            # tag reports in schedule email jobs
            scheduledMails = GoodData.get('/gdc/md/' + object.new_project_pid + '/query/scheduledmails/')
            scheduledMails["query"]["entries"].each { |x|
              scheduleMail = GoodData.get(x["link"])
              scheduleMail["scheduledMail"]["content"]["attachments"].each { |y|
                if !y["reportAttachment"].nil?
                  # attachements are reports, tag each report
                  scheduledObject = GoodData.get(y["reportAttachment"]["uri"])
                  scheduledObject["report"]["meta"]["tags"] = scheduledObject["report"]["meta"]["tags"] + ' migrated_schedEmail'
                  GoodData.post(scheduledObject["report"]["meta"]["uri"], scheduledObject)
                else !y["dashboardAttachment"].nil?
                  # attachments are dashboards
                  tabs = y["dashboardAttachment"]["tabs"]
                  scheduledObject = GoodData.get(y["dashboardAttachment"]["uri"])
                  # only tag reports in dashboard tabs that are scheduled
                  tabs.each { |t|
                    scheduledObject["projectDashboard"]["content"]["tabs"].select { |dt| dt["identifier"] == t}.each { |st|
                      st["items"].each { |i|
                        # only tag reports
                        if !i["reportItem"].nil?
                          report = GoodData.get(i["reportItem"]["obj"])
                          report["report"]["meta"]["tags"] = report["report"]["meta"]["tags"] + ' migrated_schedEmail'
                          GoodData.post(report["report"]["meta"]["uri"], report)
                        end
                      }
                    }
                  }
                end
              }
            }

            object.status = Object.TAGGED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.warn "Tagging metrics for #{object.new_project_pid} has failed. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.warn "Tagging metrics for #{object.new_project_pid}  has failed and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            $log.warn "Unknown error - The maql could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{e.message}"
          end
        end

      end
    end

    def execute_maql
      inf = Time.now.inspect  + " - executing update maql"
      puts(inf)
      $log.info inf

      fail "Cannot find MAQL file" if !File.exist?(@settings_maql_file)
      maql_source = File.read(@settings_maql_file)
      Storage.object_collection.each do |object|
        if (object.status == Object.FIXED_DATE_FACTS)
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
            $log.warn "The maql could not be applied on project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.warn "The maql could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.warn "Unknown error - The maql could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
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
                for_check.status = Object.FIXED_DATE_FACTS
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
            for_check.status = Object.FIXED_DATE_FACTS
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


    def rename_date_facts
      inf = Time.now.inspect  + " - renaming date facts"
      puts(inf)
      $log.info inf

      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.TAGGED && object.type != "template")
          GoodData.project = object.new_project_pid

          create = GoodData::Fact["dt.zendesktickets.createdat"]
          #create.identifier = "fact.zendesktickets.createdat"
          #create.save
          createObj = GoodData.get(create.uri)
          createObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.createdat"
          obj(create.uri, createObj)

          update = GoodData::Fact["dt.zendesktickets.updatedat"]
          updateObj = GoodData.get(update.uri)
          updateObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.updatedat"
          GoodData.put(update.uri, updateObj)

          assign = GoodData::Fact["dt.zendesktickets.assignedat"]
          assignObj = GoodData.get(assign.uri)
          assignObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.assignedat"
          GoodData.put(assign.uri, assignObj)

          duedate = GoodData::Fact["dt.zendesktickets.duedate"]
          duedateObj = GoodData.get(duedate.uri)
          duedateObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.duedate"
          GoodData.put(duedate.uri, duedateObj)

          initiallyassignedat = GoodData::Fact["dt.zendesktickets.initiallyassignedat"]
          initiallyassignedatObj = GoodData.get(initiallyassignedat.uri)
          initiallyassignedatObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.initiallyassignedat"
          GoodData.put(initiallyassignedat.uri, initiallyassignedatObj)

          solvedat = GoodData::Fact["dt.zendesktickets.solvedat"]
          solvedatObj = GoodData.get(solvedat.uri)
          solvedatObj["fact"]["meta"]["identifier"] = "fact.zendesktickets.solvedat"
          GoodData.put(solvedat.uri, solvedatObj)
          object.status = Object.RENAME_DATE_FACT
          Storage.store_data
        else 
          object.status = Object.FILE_UPLOAD_FINISHED
        end

      end
    end


    def upload_file(continue = false)
      inf = Time.now.inspect  + " - uploading data to datasets"
      puts(inf)
      $log.info inf

      connect_for_work()

      # If we are not continuing, lets reset everything to beginning state
      Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
        object.uploads = []
        @settings_upload_files.each do |file|
          object.uploads << {"name" => file.keys.first,"path" => file.values.first, "status" => Object.UPLOAD_NEW}
        end
      end

      Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
        @settings_upload_files.each do |file|
          GoodData.connection.upload(file.values.first,{:directory => file.keys.first,:staging_url => @connection_webdav +  "/uploads/#{object.new_project_pid}/"})
        end
      end

      while (Storage.object_collection.find_all{|o| o.status == Object.MAQL }.count > 0)
        Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
          running_task = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_RUNNING}
          if (running_task.nil?)
            new_upload = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_NEW}
            if (!new_upload.nil?)
              json = {
                  "pullIntegration" => "/#{object.new_project_pid}/#{new_upload["name"]}"
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
             
              object.status = Object.FILE_UPLOAD_FINISHED
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


    def replace_satisfaction_values
      inf = Time.now.inspect  + " - updating satisfaction metrics"
      puts(inf)
      $log.info inf

      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.FILE_UPLOAD_FINISHED )
          GoodData.project = object.new_project_pid
          satisfactionScore = GoodData::Attribute.find_first_by_title('Ticket Satisfaction Score')
          usedby = GoodData.get("/gdc/md/#{object.new_project_pid}/usedby2/5750")
          links = usedby["entries"].select do |x|
            x["category"]=="metric"
          end.select do |x|
            x["link"]!="/gdc/md/#{object.new_project_pid}/obj/6349"
          end.map { |x| x['link'] }

          links.each { |x|
            metric = GoodData::Metric[x]
            metric.replace_value(satisfactionScore.primary_label, 'Not%20Offered', 'Unoffered')
            metric.save
          }
          object.status = Object.REPLACE_SATISFACTION_VALUES
          Storage.store_data
        end
      end
    end

  #  elsif (object.status == Object.CREATED and object.type == "template")
  #  #Lets fake that the project was imported, because in case of template we are not importing
  #  #Moving directly after the Parial metada import export, because none of this task is done for template projects
  #  object.status = Object.REPLACE_SATISFACTION_VALUES
  #  Storage.store_data
  #end


    def apply_color_template
      inf = Time.now.inspect  + " - uploading custom colour palettes"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.FILE_UPLOAD_FINISHED and !@settings_color_palete.nil?)
          begin
            result = GoodData.put("/gdc/projects/#{object.new_project_pid}/styleSettings", @settings_color_palete)
            if (object.status = Object.COLOR_TEMPLATE and object.type == "migration")
              object.status = Object.COLOR_TEMPLATE
            elsif (object.status = Object.COLOR_TEMPLATE and object.type == "template")
              object.status = Object.SWAP_LABELS_DASHBOARD
            end
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
        elsif (object.status == Object.FILE_UPLOAD_FINISHED and @settings_color_palete.nil?)
          object.status = Object.COLOR_TEMPLATE
          Storage.store_data
        end
      end
    end

    def execute_partial
      inf = Time.now.inspect  + " - executing partial md import of the new dashboard"
      puts(inf)
      $log.info inf

      fail "The partial metada import token is empty" if @settings_import_token.nil? or @settings_import_token == ""
      Storage.object_collection.each do |object|
        if (object.status == Object.COLOR_TEMPLATE)
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
                for_check.status = Object.COLOR_TEMPLATE
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
            for_check.status = Object.COLOR_TEMPLATE
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

    def fix_date_facts
      inf = Time.now.inspect  + " - fixing date facts metrics"
      puts(inf)
      $log.info inf

      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.RENAME_DATE_FACT && object.type != "template")
          project_pid = object.new_project_pid
          GoodData.with_project(project_pid) do |project|
            
            err = 'false'
            standard_metrics = []
            facts = []

            begin 
              createdatfact = GoodData::Fact['fact.zendesktickets.createdat']
              initiallyassignedat = GoodData::Fact['fact.zendesktickets.initiallyassignedat']
              updatedatfact = GoodData::Fact['fact.zendesktickets.updatedat']
              solvedatfact = GoodData::Fact['fact.zendesktickets.solvedat']
              duedatefact = GoodData::Fact['fact.zendesktickets.duedate']
              assignedatfact = GoodData::Fact['fact.zendesktickets.assignedat']

              facts << createdatfact
              facts << initiallyassignedat
              facts << updatedatfact
              facts << solvedatfact
              facts << duedatefact
              facts << assignedatfact

              createdat_uri = createdatfact.uri
              initiallyassignedat_uri = initiallyassignedat.uri
              created_d_uri = GoodData::Attribute['created.date'].uri
              ticketid_uri = GoodData::Attribute['attr.zendesktickets.ticketid'].uri


              ######## FIX standard metrics

              begin
                ticketagemax = GoodData::Metric['avaRleKObsIR']
                ticketagemax.expression = "SELECT MAX({Today} - [#{created_d_uri}]) BY [#{ticketid_uri}]"
                ticketagemax.save

                standard_metrics << ticketagemax.uri
              rescue
                err = 'true'
              end

              begin
                ticketageavg = GoodData::Metric['aiBRjo2NbsDq']
                ticketageavg.expression = "SELECT AVG({Today} - [#{created_d_uri}]) BY [#{ticketid_uri}]" 
                ticketageavg.save

                standard_metrics << ticketageavg.uri
              rescue
                err = 'true'
              end

              begin
                assigntimemin = GoodData::Metric['age0HBIva1aP']
                assigntimemin.expression = "SELECT AVG([#{initiallyassignedat_uri}] - [#{createdat_uri}]) BY [#{ticketid_uri}] WHERE [/gdc/md/#{project_pid}/obj/1305]<>[/gdc/md/#{project_pid}/obj/1305/elements?id=9]  AND IFNULL([#{initiallyassignedat_uri}],0) > 0"
                assigntimemin.save

                standard_metrics << assigntimemin.uri
              rescue
                err = 'true'
              end

              begin
                assigntimehrs = GoodData::Metric['afz0OdGSffEC']
                assigntimehrs.expression = "SELECT AVG([#{initiallyassignedat_uri}] - [#{createdat_uri}])/60 BY [#{ticketid_uri}] WHERE [/gdc/md/#{project_pid}/obj/1305]<>[/gdc/md/#{project_pid}/obj/1305/elements?id=9]  AND IFNULL([#{initiallyassignedat_uri}],0) > 0"
                assigntimehrs.save

                standard_metrics << assigntimehrs.uri
              rescue
                err = 'true'
              end

              ######### TAG non-standard metrics

              facts.each {  |f|
                 f.used_by('metric').select { |m| !standard_metrics.include?(m['link']) }.each { |mm|

                o = GoodData::Metric[mm['link']]
                o.tags = o.tags + ' migrated_checkDateFacts'
                o.save

                }

               }

            rescue  => e
              pp e
            end

          object.status = Object.FIXED_DATE_FACTS
          Storage.store_data

          end
        end
      end
    end


    def swap_labels
      inf = Time.now.inspect  + " - swapping labels in reports and dashboards"
      puts(inf)
      $log.info inf

      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.PARTIAL)
        #if (object.status != "v prdeli")
          project_pid = object.new_project_pid
          label_from = ''
          attr_from = ''
          label_to = ''
          GoodData.use project_pid
          #start config loop here
          @settings_swap_config.each do |s|
            attr = GoodData::Attribute[s["attribute"]]
            attr_from = attr.uri.split("/").last

            attr.labels.each do |x|
              if x.meta["identifier"] == s["label_from"]

                label_from = x.meta["uri"].split("/").last
              end
              if x.meta["identifier"] == s["label_to"]
                label_to = x.meta["uri"].split("/").last
              end
            end

            GoodData.with_project(project_pid) do |project|
              linehash = {}
              usedby = GoodData.get("/gdc/md/#{project_pid}/usedby2/" + label_from)
              links = usedby["entries"].select {|x| x["category"]=="reportDefinition"}.map { |x| x['link'] }
              definitions = links.map {|x| GoodData.get(x)}
              what = "/gdc/md/#{project_pid}/obj/#{label_from}"
              whatPatt = /(\/gdc\/md\/#{project_pid}\/obj\/#{attr_from}\/elements?)/


              report_definition_to_tag = []
              definitions.each do |x|
                jj = JSON.generate(x).gsub(/\"#{what}\"/,"\"/gdc/md/#{project_pid}/obj/#{label_to}\"")
                payload = JSON.parse(jj)
                res = GoodData.put(x["reportDefinition"]["meta"]["uri"],payload)
               end

              ########## tag reports which have label_from in filter
              usedby = GoodData.get("/gdc/md/#{project_pid}/usedby2/" + attr_from)
              links = usedby["entries"].select {|x| x["category"]=="reportDefinition"}.map { |x| x['link'] }
              definitions = links.map {|x| GoodData.get(x)}

              definitions.each do |x|
                x["reportDefinition"]["content"]["filters"].each do |f|
                    if f["expression"] =~ whatPatt
                      report_definition_to_tag << x["reportDefinition"]["meta"]["uri"].split("/").last
                    end
                end
              end

              report_definition_to_tag.each do |report_definition_id|
                usedby = GoodData.get("/gdc/md/#{project_pid}/usedby2/#{report_definition_id}" )
                links = usedby["entries"].select {|x| x["category"]=="report"}
                links.each do |link|
                  report = GoodData::Report[link["link"].split("/").last]

                  if !report.meta.key?('locked') 
                    report.tags += " migrated_FixFilter"
                    report.save                  
                  end

                  
                end
              end

              # swap dashboard filters
              dashboards = GoodData::Dashboard[:all]
              dashboards.each do |x|
                dd = GoodData::Dashboard[x["link"]]
                dd.content["filters"].each do |x|
                  if x["filterItemContent"]["obj"] == what
                    # puts("Replacing " + x["filterItemContent"]["obj"] + " with " +  x["filterItemContent"]["obj"].gsub(/#{what}/,"/gdc/md/#{project_pid}/obj/#{label_to}") )
                    x["filterItemContent"]["obj"] = x["filterItemContent"]["obj"].gsub(/#{what}/,"/gdc/md/#{project_pid}/obj/#{label_to}")
                  end
                end
                dd.save
              end
            end
          end
          object.status = Object.SWAP_LABELS
          Storage.store_data
        end
      end
    end


    def swap_label_dash_filters
      inf = Time.now.inspect  + " - swapping dashboard filters"
      puts(inf)
      $log.info inf

      connect_for_work()
      Storage.object_collection.each do |object|
        if (object.status == Object.SWAP_LABELS)
          project_pid = object.new_project_pid
          label_from = ''
          label_to = ''

          GoodData.use project_pid
          #start config loop here
          @settings_swap_config.each do |s|
            attr = GoodData::Attribute[s["attribute"]]
            attr.labels.each do |x|
              if x.meta["identifier"] == s["label_from"]
                label_from = x.meta["uri"].gsub("/gdc/md/#{project_pid}/obj/","")
              end
              if x.identifier == s["label_to"]
                label_to = x.uri.gsub("/gdc/md/#{project_pid}/obj/","")
              end
            end
            what = "/gdc/md/#{project_pid}/obj/#{label_from}"
            dashboards = GoodData::Dashboard[:all]
            dashboards.each do |x|
              dd = GoodData::Dashboard[x["link"]]
              dd.content["filters"].each do |x|
                if x["filterItemContent"]["obj"] == what
                  # puts("Replacing " + x["filterItemContent"]["obj"] + " with " +  x["filterItemContent"]["obj"].gsub(/#{what}/,"/gdc/md/#{project_pid}/obj/#{label_to}") )
                  x["filterItemContent"]["obj"] = x["filterItemContent"]["obj"].gsub(/#{what}/,"/gdc/md/#{project_pid}/obj/#{label_to}")
                end
              end
              dd.save
            end
          end
          object.status = Object.SWAP_LABELS_DASHBOARD
          Storage.store_data
        end
      end
    end

    def create_user
      inf = Time.now.inspect  + " - creating connector users"
      puts(inf)
      $log.info inf


      fail "You need to specify Zendesk domain name" if @settings_domain.nil?
      users = GoodData::Domain.users(@settings_domain)
      user_entity = users.find{|u| u.login == @settings_user_to_add}
      Storage.object_collection.each do |object|
        if (object.status == Object.SWAP_LABELS_DASHBOARD)

          #Get roles in current project
          project = GoodData::Project[object.new_project_pid]
          # lets find the connector role
          roles = GoodData.get("/gdc/projects/#{object.new_project_pid}/roles")
          connectorRoleUrl = ""
          
          roles["projectRoles"]["roles"].each do |role|
            role_response = GoodData.get(role)
            if (role_response["projectRole"]["meta"]["identifier"] == "connectorsSystemRole") 
              connectorRoleUrl = role
              break
            end
          end
          json =
              {
                "user" => {
                  "content" => {
                      "status" => "ENABLED",
                      "userRoles" =>["#{connectorRoleUrl}"]
                  },
                  "links"   => {
                      "self" => user_entity.json['accountSetting']['links']['self']
                  }
              }
              }

          begin
            GoodData.post("/gdc/projects/#{object.new_project_pid}/users",json)
            object.status = Object.USER_CREATED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "I could not add user to project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "I could not add user to project #{object.new_project_pid}. The API returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - I could not add user to project #{object.new_project_pid} and returned 500. Reason: #{response["message"]}"
          end
        end
      end


    end


    def create_integration
      inf = Time.now.inspect  + " - creating ZD4 integrations"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.USER_CREATED)

          begin
            json = {
                "integration" => {
                    "projectTemplate" => "/projectTemplates/ZendeskAnalytics/10",
                    "active" => true
                }
            }

            if (object.rerun.nil? or object.rerun == false)
              result = GoodData.post("/gdc/projects/#{object.new_project_pid}/connectors/zendesk4/integration", json)
            else
              result = GoodData.put("/gdc/projects/#{object.new_project_pid}/connectors/zendesk4/integration", json)
            end
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

    
    
    
    def dummy
      Storage.object_collection.each do |object|
        if (object.status == Object.PARTIAL)
            object.rerun = true
            object.new_project_pid = object.old_project_pid
            object.status = Object.USER_CREATED
        end        
        Storage.store_data
      end
    end


    def create_endpoint
      inf = Time.now.inspect  + " - setting up ZD4 integrations"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.INTEGRATION_CREATED)

          json = {
              "settings" => {
                 "apiUrl" => object.api_url
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
      inf = Time.now.inspect  + " - kicking off the ZD4 integrations"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.ENDPOINT_SET)
          json = {
              "process" => {"incremental" => false}
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
    
    
    
    def check_mufs_in_projects
      inf = Time.now.inspect + " checking mandatory user filters in projects"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if !object.isFilterChecked
          begin
            pid = object.old_project_pid
            json = GoodData.get("/gdc/md/#{pid}/query/userfilters")
            if (json["query"]["entries"].count > 0)
              object.hasMandatoryUserFilter = true
            else
              object.hasMandatoryUserFilter = false
            end
            object.isFilterChecked = true
            Storage.store_data
          rescue => e
            $log.error "There was some issues while checking Mandatory User Filters in #{object.old_project_pid} project!"
            Storage.store_data
          end
        end
      end
    end
    
    
    def check_variables_in_projects
      inf = Time.now.inspect + " checking variales in projects"
      puts(inf)
      $log.info inf
      
      Storage.object_collection.each do |object|
        if !object.isVariableChecked
          begin
            pid = object.old_project_pid
            json = GoodData.get("/gdc/md/#{pid}/query/prompts")
            if (json["query"]["entries"].count > 0)
              object.hasVariable = true
            else
              object.hasVariable = false
            end
            object.isVariableChecked = true
            Storage.store_data
          rescue => e
            $log.error "There was some issues while checking Mandatory User Filters in #{object.old_project_pid} project!"
            Storage.store_data            
          end       
        end
      end
    end 
    
    def change_type_sanitize
      Storage.object_collection.each do |object|
        if (object.status == Object.MAQL)
          begin
            # change country
            request = {
                "identifierToUri" => ["label.ticketupdate.country"]
            }

            response = GoodData.post("/gdc/md/#{object.old_project_pid}/identifiers", request)
            if (response["identifiers"].count > 0)
              url = response["identifiers"].first["uri"]
              json = GoodData.get(url)

              json["attributeDisplayForm"]["content"]["type"] = "GDC.geo.worldcountries.name"
              response = GoodData.post(url, json)
            end

            # change pin
            request = {
                "identifierToUri" => ["label.ticketupdate.geopushpin"]
            }

            response = GoodData.post("/gdc/md/#{object.old_project_pid}/identifiers", request)
            if (response["identifiers"].count > 0)
              url = response["identifiers"].first["uri"]
              json = GoodData.get(url)

              json["attributeDisplayForm"]["content"]["type"] = "GDC.geo.pin"
              response = GoodData.post(url, json)
            end

            object.status = Object.TYPE_CHANGED
            Storage.store_data
          rescue => e
            object.status = Object.MAQL
            Storage.store_data
          end
        end
      end
    end
        
    
    def rename_factofs_identifier
      inf = Time.now.inspect + " - executing the factof renaming"
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          begin
            # set a project pid
            project_pid = object.old_project_pid
            # use a GoodData project
            GoodData.use project_pid
            # read the factof element details
            attr = GoodData::Attribute["attr.zendeskticketsbacklog.factsof"]
            # obj check
            obj = GoodData::get(attr.uri)
            # rename object
            obj["attribute"]["meta"]["identifier"] = "attr.zendeskticketsbacklog.ticketbackloghistory"
            # push the change
            GoodData.put(attr.uri, obj)
            # update the persistent file
            object.status = Object.TAGGED
            # save the file
            Storage.store_data
          rescue => e
            response = JSON.load(e.response)
            $log.warn "The update of the identifier was not successful. Reason: #{response["error"]["message"]}"
          end
        end
      end
    end
    
    def execute_maql_sanitize      
      inf = Time.now.inspect  + " - executing update maql"
      fail "Cannot find MAQL file" if !File.exist?(@settings_maql_file)
      maql_source = File.read(@settings_maql_file)
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          maql = {
              "manage" => {
                  "maql" => maql_source
              }

          }
          begin
            result = GoodData.post("/gdc/md/#{object.old_project_pid}/ldm/manage2", maql)
            task_id = result["entries"].first["link"].match(/.*\/tasks\/(.*)\/status/)[1]
            object.maql_update_task_id = task_id
            object.status = Object.MAQL_REQUESTED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.warn "The maql could not be applied on project #{object.old_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.warn "The maql could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.warn "Unknown error - The maql could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          end

          while (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count >= @settings_number_simultanious_projects)
            $log.info "Waiting till all MAQL is applied on all projects"
            Storage.get_objects_by_status(Object.MAQL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                $log.info "MAQL for project #{for_check.old_project_pid} successfully applied"
                for_check.status = Object.MAQL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.NEW
                Storage.store_data
                $log.error "Applying MAQL on project #{for_check.old_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
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
          result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            $log.info "MAQL for project #{for_check.old_project_pid} successfully applied"
            for_check.status = Object.MAQL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.NEW
            Storage.store_data
            $log.error "Applying MAQL on project #{for_check.old_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
    end


    def load_dataset_sanitize

        inf = Time.now.inspect  + " - uploading data to datasets"
        puts(inf)
        $log.info inf

        connect_for_work()

        # If we are not continuing, lets reset everything to beginning state
        Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
          object.uploads = []
          @settings_upload_files_sanitize.each do |file|
            object.uploads << {"name" => file.keys.first,"path" => file.values.first, "status" => Object.UPLOAD_NEW}
          end
        end
        Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
          @settings_upload_files_sanitize.each do |file|
            GoodData.connection.upload(file.values.first,{:directory => file.keys.first,:staging_url => @connection_webdav +  "/uploads/#{object.old_project_pid}/"})
          end
        end

        while (Storage.object_collection.find_all{|o| o.status == Object.MAQL }.count > 0)
          Storage.object_collection.find_all{|o| o.status == Object.MAQL}.each do |object|
            running_task = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_RUNNING}
            if (running_task.nil?)
              new_upload = object.uploads.find{|upload| upload["status"] == Object.UPLOAD_NEW}
              if (!new_upload.nil?)
                json = {
                    "pullIntegration" => "/#{object.old_project_pid}/#{new_upload["name"]}"
                }
                begin
                  res = GoodData.post("/gdc/md/#{object.old_project_pid}/etl/pull", json)
                  new_upload["uri"] = res["pullTask"]["uri"]
                  new_upload["status"] = Object.UPLOAD_RUNNING
                  running_task = new_upload
                rescue RestClient::BadRequest => e
                  response = JSON.load(e.response)
                  new_upload["status"] = Object.UPLOAD_ERROR
                  $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.old_project_pid}. Reason: #{response["error"]["message"]}"
                rescue RestClient::InternalServerError => e
                  response = JSON.load(e.response)
                  new_upload["status"] = Object.UPLOAD_ERROR
                  $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.old_project_pid}. Reason: #{response["error"]["message"]}"
                rescue => e
                  new_upload["status"] = Object.UPLOAD_ERROR
                  $log.warn "Upload of file #{new_upload["name"]} has failed for project #{object.old_project_pid}.. Reason: Unknown reason"
                end
                Storage.store_data
              else
                object.status = Object.FILE_UPLOAD_FINISHED
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
                $log.warn "Upload of file #{running_task["name"]} has failed for project #{object.old_project_pid}. Reason: Unknown reason"
              end
              Storage.store_data
            end
          end
          sleep(5)
        end
    end
    
    def execute_partial_sanitize
      inf = Time.now.inspect  + " - executing partial md import of the new dashboard"
      puts(inf)
      $log.info inf

      fail "The partial metada import token is empty" if @settings_import_token.nil? or @settings_import_token == ""
      Storage.object_collection.each do |object|
        if (object.status == Object.UPLOAD_OK)
          json = {
              "partialMDImport" => {
                  "token" => "#{@settings_import_token}",
                  "overwriteNewer" => "1",
                  "updateLDMObjects" => "0"
              }
          }
          begin
            result = GoodData.post("/gdc/md/#{object.old_project_pid}/maintenance/partialmdimport", json)
            task_id = result["uri"].match(/.*\/tasks\/(.*)\/status/)[1]
            object.partial_metadata_task_id = task_id
            object.status = Object.PARTIAL_REQUESTED
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The partial metadata could not be applied on project #{object.old_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The partial metadata could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The partial metadata could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["message"]}"
          end


          while (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > @settings_number_simultanious_projects)
            $log.info "Waiting till all Partial Metadata is applied on all projects"
            Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                $log.info "Partial Metadata for project #{for_check.old_project_pid} successfully applied"
                for_check.status = Object.PARTIAL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.UPLOAD_OK
                Storage.store_data
                $log.error "Applying Partial Metadata on project #{for_check.old_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
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
          result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            $log.info "Partial Metadata for project #{for_check.old_project_pid} successfully applied"
            for_check.status = Object.PARTIAL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.MAQL
            Storage.store_data
            $log.error "Applying Partial Metadata on project #{for_check.old_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > 0)
          $log.info "Waiting - START"
          sleep(10)
          $log.info "Waiting - STOP"
        end
      end
    end

    def unlocking_metric_reports
      inf = Time.now.inspect  + " - unlocking metrics"
      puts (inf)
      $log.info inf
      Storage.object_collection.each do |object|
        if (object.status == Object.NEW)
          begin
            # work with project
            GoodData.project = object.old_project_pid         
            # metrics change
            metrics = GoodData::Metric[:all]
            # iterate over
            metrics.each do |metric|
              # obj check
              obj = GoodData::get(metric["link"])
              # rename 
              if (obj["metric"]["meta"]["locked"] == 1)
                # change value
                obj["metric"]["meta"]["locked"] = 0
                # push the change
                GoodData.put(metric["link"], obj)
              end
            end

            # read all reports from the project
            reports = GoodData::Report[:all]
            # iterate over
            reports.each do |report|
              # obj check
              obj = GoodData::get(report["link"])
              # rename object in case of locked settings is true
              if (obj["report"]["meta"]["locked"] == 1)
                # change the value
                obj["report"]["meta"]["locked"] = 0
                # push the change
                GoodData.put(report["link"], obj)
              end  
            end
            # update the persistent file
            object.status = Object.FINISHED
            # save the file
            Storage.store_data        
          rescue => e
            response = JSON.load(e.response)
            $log.warn "Unknown error - The identifier couldn't be changed and returned 500. Reason: #{response["error"]["message"]}"      
          end
        end
      end
    end
    
    def create_endpoint_sanitize
      inf = Time.now.inspect  + " - setting up ZD4 integrations"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.INTEGRATION_CREATED)

          json = {
              "settings" => {
                 "apiUrl" => object.api_url
            }
          }
          begin
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


    def run_integration_sanitize
      inf = Time.now.inspect  + " - kicking off the ZD4 integrations"
      puts(inf)
      $log.info inf

      Storage.object_collection.each do |object|
        if (object.status == Object.PARTIAL)
          json = {
              "process" => {"incremental" => false}
          }
          begin
            result = GoodData.post("/gdc/projects/#{object.old_project_pid}/connectors/zendesk4/integration/processes", json)
            object.status = Object.ENDPOINT_SET_FINISHED
            object.zendesk_sync_process = result["uri"]
            Storage.store_data
          rescue RestClient::BadRequest => e
            response = JSON.load(e.response)
            $log.error "The zendesk process for project #{object.old_project_pid} could not be started. Reason: #{response["error"]["message"]}"

          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            $log.error "The zendesk process for project #{object.old_project_pid} could not be started. Returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            $log.error "Unknown error - The zendesk process for project #{object.old_project_pid} could not be started and returned 500. Reason: #{response["message"]}"
          end
        end
      end
    end

    def set_proper_status_sanitize
      Storage.object_collection.each do |object|
        object.status = Object.TAGGED
        Storage.store_data
      end
    end





  end
end