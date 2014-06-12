$: << File.expand_path(File.dirname(File.realpath(__FILE__))  )
require 'migration.rb'
require 'pp'
require 'gooddata'

STORAGE_FILE_PATH = "../storage/storage.yaml"
META_FILE_PATH = "meta.json"
@object_collection = []
PROJECTS = {}

connect_for_work()

if (File.exist?(META_FILE_PATH))
    json = JSON.load( File.open(META_FILE_PATH) )
	PROJECTS = json['projects']
end


if (File.exist?(STORAGE_FILE_PATH))
    $/="\n\n"
    File.open(STORAGE_FILE_PATH, "r").each do |object|
    	yaml = YAML::load(object)
    	@object_collection << yaml
    end
end

@object_collection.each do |obj|

	if PROJECTS[obj.new_project_pid] != 'DONE'

		begin

			res = GoodData.get(obj.zendesk_sync_process)
			PROJECTS[obj.new_project_pid] = res["process"]["status"]["code"]

			# TODO, check the process result. If it's finished (SYNCHRONIZED) call the upgrade endpoints 
			#(see bellow) and set the project status in storage.yaml and meta.json to DONE.
			# If it's not finished, skip it.
			# Nn both cases write the result to csv


			# 1, Spojeni zendeskoveho uctu s nasim projektem

			# - POST > /gdc/projects/<projectName>/connectors/zendesk4/integration/settings/push
			# - payload: {"push":{}}
			# - result: 200

			# Spoji dany projekt <projectName> se Zendeskovym uctem, ktery by v tehle chvili uz mel byt nastaveny (pri migraci pomoci PUT na /gdc/projects/<projectName>/connectors/zendesk4/integration/settings/push)


			# 2, Spojeni noveho Zendesk4 projektu se Zendesk3 originalem

			# - PUT > /gdc/projects/<projectName>/connectors/zendesk4/integration/settings
			# - payload: {"settings" : {"createdFrom": "/gdc/projects/sder4et5trgtrg"}}
			# - result: 200

			# Spoji zmigrovany zendesk4 projekt s puvodnim zendesk3 projektem (v nasi databazi, abychom to meli pod kontrolou).




			# output to csv instead of puts would be great
			puts "[Processing ] " + obj.new_project_pid + " (" + res["process"]["status"]["code"] + ")"

		rescue

			PROJECTS[obj.new_project_pid] = 'ERR'
			puts "[Skipping]   " + obj.new_project_pid + " (ERR)"

		end

	else

		puts "[Skipping]   " + obj.new_project_pid + " (DONE)"

	end	
end


File.open(META_FILE_PATH,"w") do |file|
		json = {}
		json["projects"] = PROJECTS
        file.puts JSON.dump(json)
    
end