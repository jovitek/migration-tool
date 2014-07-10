require "gooddata"
require "time"
require "yaml"
%w(storage object).each {|a| require "./lib/persistent/#{a}"}

module Migration
  class ProjectSanitizer


    def initialize
      file = "config/config.json"
      fail "The config file don't exists" if !File.exists?("#{file}")
      File.open("#{file}", "r") do |f|
        json = JSON.load(f)
        @connection_username = json["connection"]["login"]
        @connection_password = json["connection"]["password"]
        @connection_server = json["connection"]["server"]
        @settings_maql_file = json["settings"]["maql_file"]
        @settings_import_token = json["settings"]["import_token"]
        @settings_project_file = json["settings"]["project_file"]
        @settings_number_simultanious_projects = json["settings"]["number_simultanious_projects"]
      end

      Storage.load_data
      Storage.store_data
    end

    def connect
      GoodData.connect(@connection_username, @connection_password, {:server => @connection_server})
    end

    def disconnect
      GoodData.disconnect()
    end

    def load_data_source
      fail "Project file doesn't exists" if !File.exists?(@settings_project_file)
      pids = []
      CSV.foreach(@settings_project_file, {:headers => true, :skip_blanks => true}) do |csv_obj|
        pids.push({"pid" => csv_obj["project_pid"]})
      end

      pids.uniq! {|p| p["pid"]}
      pids.each do |hash|
        object = Storage.get_object_by_old_project_pid(hash["pid"])
        if (object.nil?)
          object = Object.new()
          object.old_project_pid = hash["pid"]
          object.status = Object.NEW
          Storage.add_object(object)
        end
      end
    end

    def execute_maql
      #inf = Time.now.inspect  + " - executing update maql"
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
            #$log.warn "The maql could not be applied on project #{object.old_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            #$log.warn "The maql could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            #$log.warn "Unknown error - The maql could not be applied on project #{object.old_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          end

          while (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count >= @settings_number_simultanious_projects)
            #$log.info "Waiting till all MAQL is applied on all projects"
            Storage.get_objects_by_status(Object.MAQL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                #$log.info "MAQL for project #{for_check.new_project_pid} successfully applied"
                for_check.status = Object.MAQL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.NEW
                Storage.store_data
                #$log.error "Applying MAQL on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
              end
            end

            if (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count >= @settings_number_simultanious_projects)
              #$log.info "Waiting - START"
              sleep(10)
              #$log.info "Waiting - STOP"
            end
          end
        end
      end

      while (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count > 0)
        #$log.info "Waiting till all MAQL is applied on all projects"
        Storage.get_objects_by_status(Object.MAQL_REQUESTED).each do |for_check|
          result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.maql_update_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            #$log.info "MAQL for project #{for_check.new_project_pid} successfully applied"
            for_check.status = Object.MAQL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.NEW
            Storage.store_data
            #$log.error "Applying MAQL on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.MAQL_REQUESTED).count > 0)
          #$log.info "Waiting - START"
          sleep(10)
          #$log.info "Waiting - STOP"
        end
      end
    end

    def change_type
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

    def execute_partial
      #inf = Time.now.inspect  + " - executing partial md import of the new dashboard"
      #puts(inf)
      #$log.info inf

      fail "The partial metada import token is empty" if @settings_import_token.nil? or @settings_import_token == ""
      Storage.object_collection.each do |object|
        if (object.status == Object.TYPE_CHANGED)
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
            #$log.error "The partial metadata could not be applied on project #{object.new_project_pid}. Reason: #{response["error"]["message"]}"
          rescue RestClient::InternalServerError => e
            response = JSON.load(e.response)
            #$log.error "The partial metadata could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["error"]["message"]}"
          rescue => e
            response = JSON.load(e.response)
            #$log.error "Unknown error - The partial metadata could not be applied on project #{object.new_project_pid} and returned 500. Reason: #{response["message"]}"
          end


          while (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > @settings_number_simultanious_projects)
            #$log.info "Waiting till all Partial Metadata is applied on all projects"
            Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).each do |for_check|
              result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
              status = result["wTaskStatus"]["status"]
              if (status == 'OK')
                #$log.info "Partial Metadata for project #{for_check.new_project_pid} successfully applied"
                for_check.status = Object.PARTIAL
                Storage.store_data
              elsif  (status == "ERROR")
                for_check.status = Object.TYPE_CHANGED
                Storage.store_data
                #$log.error "Applying Partial Metadata on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
              end
            end

            if (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > @settings_number_simultanious_projects)
              #$log.info "Waiting - START"
              sleep(10)
              #$log.info "Waiting - STOP"
            end
          end
        end
      end

      while (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > 0)
        #$log.info "Waiting till all Partial Metadata is applied on all projects"
        Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).each do |for_check|
          result = GoodData.get("/gdc/md/#{for_check.old_project_pid}/tasks/#{for_check.partial_metadata_task_id}/status")
          status = result["wTaskStatus"]["status"]
          if (status == 'OK')
            #$log.info "Partial Metadata for project #{for_check.new_project_pid} successfully applied"
            for_check.status = Object.PARTIAL
            Storage.store_data
          elsif  (status == "ERROR")
            for_check.status = Object.TYPE_CHANGED
            Storage.store_data
            #$log.error "Applying Partial Metadata on project #{for_check.new_project_pid} has failed - please restart \n Message: #{result["wTaskStatus"]["messages"]}"
          end
        end

        if (Storage.get_objects_by_status(Object.PARTIAL_REQUESTED).count > 0)
          #$log.info "Waiting - START"
          sleep(10)
          #$log.info "Waiting - STOP"
        end
      end
    end


  end
end

