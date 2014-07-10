# A simple checker for GoodData Zendesk projects whether they contain user filters or not
#
# (c) 2014 GoodData corporation

require "GoodData"
require "Time"
require "yaml"
%w(storage object).each {|a| require "./lib/persistent/#{a}"}


module Migration
  class MufChecker

    def initialize
      file = "config/config.json"
      fail "The config file don't exists" if !File.exists?("#{file}")
      File.open("#{file}", "r") do |f|
        json = JSON.load(f)
        @connection_export_username = json["connection"]["export_login"]
        @connection_export_password = json["connection"]["export_password"]
        @settings_project_file = json["settings"]["project_file"]
      end

      #GoodData.logger = $log
      #GoodData.logger.level = Logger::DEBUG

      Storage.load_data
      Storage.store_data
    end

    def connect
      GoodData.connect(@connection_export_username, @connection_export_password)
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
          object.isFilterChecked = false;
          Storage.add_object(object)
        end
      end
    end

    def check_mufs_in_projects
      inf = Time.now.inspect + " checking mandatory user filters in projects"
      puts inf
      #$log.info = inf

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
            #$log.warn "There was some issues while checking #{object.old_project_pid} project"
            Storage.store_data
          end
        end
      end
    end
  end
end

