module Migration


  class Object

    attr_accessor :old_project_pid,:new_project_pid,:status,:export_token,:export_status_url,:token_validity,:title,:summary,:import_status_url,:maql_update_task_id,:partial_metadata_task_id,:zendesk_sync_process,:type

    def self.VALID_TYPES
      ["migration","template"]
    end

    def self.NEW
      "NEW"
    end

    def self.PROVISIONING
      "PROVISIONING"
    end

    def self.CREATED
      "CREATED"
    end

    def self.CLONE_REQUESTED
      "CLONED_REQUESTED"
    end

    def self.CLONED
      "CLONED"
    end

    def self.IMPORT_REQUESTED
      "IMPORT_REQUESTED"
    end

    def self.IMPORTED
      "IMPORTED"
    end

    def self.MAQL_REQUESTED
      "MAQL_REQUESTED"
    end


    def self.MAQL
      "MAQL"
    end

    def self.PARTIAL_REQUESTED
      "PARTIAL_REQUESTED"
    end

    def self.PARTIAL
      "PARTIAL"
    end

    def self.INTEGRATION_CREATED
      "INTEGRATION_CREATED"
    end

    def self.ENDPOINT_SET
      "ENDPOINT_SET"
    end

    def self.FINISHED
      "FINISHED"
    end


  end





end