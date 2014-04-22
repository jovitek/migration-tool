module Migration


  class Storage

    def self.load_data
      @object_collection = []
      if (File.exist?("storage/storage.yaml"))
       $/="\n\n"
       File.open("storage/storage.yaml", "r").each do |object|
         yaml = YAML::load(object)
         @object_collection << yaml
       end
     end
    end


    def self.object_collection
      @object_collection
    end

    def self.store_data
      File.open("storage/storage.yaml","w") do |file|
        @object_collection.each do |object|
          file.puts YAML::dump(object)
          file.puts ""
        end
      end
    end

    def self.add_object(object)
      @object_collection.push(object)
    end


    def self.get_object_by_old_project_pid(pid)
      @object_collection.find{|o| o.old_project_pid == pid }
    end

    def self.get_objects_by_status(status)
      @object_collection.find_all{|o| o.status == status}
    end




  end




end