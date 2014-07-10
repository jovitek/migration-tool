module Migration


  class Object

    attr_accessor :rerun,:old_project_pid,:new_project_pid,:status,:export_token,:export_status_url,:token_validity,:title,:summary,:import_status_url,:maql_update_task_id,:partial_metadata_task_id,:zendesk_sync_process,:type,:upload_finished,:uploads,:api_url,:zd_account,:isFilterChecked,:hasMandatoryUserFilter,:isVariableChecked,:hasVariable

    def self.VALID_TYPES
      ["migration","template"]
    end

    def self.NEW
      "NEW"
    end

    def self.TYPE_CHANGED
      "TYPE_CHANGED"
    end


    def self.IGNORE
      "IGNORE"
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

    def self.TAGGED
      "TAGGED"
    end

    def self.MAQL_REQUESTED
      "MAQL_REQUESTED"
    end


    def self.MAQL
      "MAQL"
    end

    def self.RENAME_DATE_FACT
      "RENAME_DATE_FACT"
    end

    def self.FILE_UPLOAD_FINISHED
      "UPLOAD_FILE_FINISHED"
    end


    def self.REPLACE_SATISFACTION_VALUES
      "REPLACE_SATISFACTION_VALUES"
    end

    def self.COLOR_TEMPLATE
      "COLOR_TEMPLATE"
    end

    def self.SWAP_LABELS
      "SWAP_LABELS"
    end

    def self.SWAP_LABELS_DASHBOARD
      "SWAP_LABELS_DASHBOARD"
    end


    def self.PARTIAL_REQUESTED
      "PARTIAL_REQUESTED"
    end

    def self.PARTIAL
      "PARTIAL"
    end


    def self.USER_CREATED
      "USER_CREATED"
    end

    def self.INTEGRATION_CREATED
      "INTEGRATION_CREATED"
    end

    def self.ENDPOINT_SET
      "ENDPOINT_SET"
    end


    def self.ENDPOINT_SET_FINISHED
      "ENDPOINT_SET_FINISHED"
    end



    def self.FINISHED
      "FINISHED"
    end

    def self.UPLOAD_NEW
      "UPLOAD_NEW"
    end

    def self.UPLOAD_RUNNING
      "UPLOAD_RUNNING"
    end

    def self.UPLOAD_OK
      "UPLOAD_OK"
    end

    def self.UPLOAD_ERROR
      "UPLOAD_ERROR"
    end

    def self.FIXED_DATE_FACTS
      "FIXED_DATE_FACTS"
    end



  end





end
