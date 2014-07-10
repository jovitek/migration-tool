require 'rubygems'
require 'gooddata'
require 'pp'
require 'json'
require 'csv'
require 'pry'

#GoodData.logging_on
#con_res = GoodData.connect("josef.vitek@gooddata.com", "")
con_res = GoodData.connect("gooddata2@zendesk.com", "",{:server => ''})

X_HEADERS = ['project']
CSV.open('file.csv', 'w') { |csv| csv<<X_HEADERS}
ERRFILE = File.open("errors.txt", "w")


def renameDtFacts ( options = {} )
	project_pid = options[:project_pid]
	
	GoodData.use project_pid
	facts = GoodData::Fact.all
	# facts = facts.map { |x| GoodData::Fact[x['link']] }

	dtf = facts.select  { |f| f['identifier'].include? "dt." }
	dtf = dtf.map { |x| GoodData::Fact[x['link']] }

	dtf.each { |f|
		f.meta['identifier']  = f.meta['identifier'].gsub("dt.", "fact.")
		f.save
		pp f.identifier
	 }
end


def renameOldFashioned ( options = {} )

	project_pid = options[:project_pid]
	
	GoodData.use project_pid

		fact = GoodData::Fact["dt.zendesktickets.createdat"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.createdat"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.zendesktickets.updatedat"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.updatedat"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.zendesktickets.assignedat"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.assignedat"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.zendesktickets.duedate"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.duedate"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.zendesktickets.initiallyassignedat"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.initiallyassignedat"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.zendesktickets.solvedat"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.solvedat"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]


        fact = GoodData::Fact["dt.zendesktickets.assigneeupdated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.assigneeupdated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

        fact = GoodData::Fact["dt.zendesktickets.requesterupdated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.zendesktickets.requesterupdated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

  		fact = GoodData::Fact["dt.organization.organizationcreated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.organization.organizationcreated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

        fact = GoodData::Fact["dt.organization.organizationupdated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.organization.organizationupdated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

        fact = GoodData::Fact["dt.updater.updatercreated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.updater.updatercreated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

        fact = GoodData::Fact["dt.requester.usercreated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.requester.usercreated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        fact = GoodData::Fact["dt.requester.userlstlogin"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.requester.userlstlogin"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]

        fact = GoodData::Fact["dt.requester.userupdated"]
        obj = GoodData.get(fact.uri)
        obj["fact"]["meta"]["identifier"] = "fact.requester.userupdated"
        GoodData.put(fact.uri, obj)
        puts project_pid + " " + obj["fact"]["meta"]["identifier"]
        
        attr = GoodData::Attribute["attr.ticketfactshistory.factsof"]
        obj = GoodData.get(attr.uri)
        obj["attribute"]["meta"]["identifier"] = "attr.ticketfactshistory.ticketfactshistory"
        GoodData.put(attr.uri, obj)
        puts project_pid + " " + obj["attribute"]["meta"]["identifier"]

        attr = GoodData::Attribute["attr.ticketattributeshistory.factsof"]
        obj = GoodData.get(attr.uri)
        obj["attribute"]["meta"]["identifier"] = "attr.ticketattributeshistory.ticketattributeshistory"
        GoodData.put(attr.uri, obj)
        puts project_pid + " " + obj["attribute"]["meta"]["identifier"]

        attr = GoodData::Attribute["attr.satisfactionhistory.factsof"]
        obj = GoodData.get(attr.uri)
        obj["attribute"]["meta"]["identifier"] = "attr.satisfactionhistory.satisfactionhistory"
        GoodData.put(attr.uri, obj)
        puts project_pid + " " + obj["attribute"]["meta"]["identifier"]



end 



CSV.foreach('lapids.csv', :headers => true, :return_headers => false) do |row|

	renameOldFashioned ({:project_pid => row['pid']})  #group
 
end

ERRFILE.close
