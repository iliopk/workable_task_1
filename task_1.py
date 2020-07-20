import pandas as pd
import re
import shlex


logs=[]
with open("2014-09-03.log","r",encoding="utf8") as f:
    for line in f:
        line=line.strip(' \n')
        logs.append(line.split("\t"))
#dataFrame holding all entries of log file
log_DF=pd.DataFrame(logs)
#dataframe holding entries for web dynos
log_app=log_DF[log_DF[8].str.contains("app/web")]
log_app_msg=log_app[log_app.columns[9]].str.replace('\x1b','')
log_app_msg=  pd.DataFrame([re.sub("[[0-9]+m","", str(x)) for x in log_app_msg])
# create new column for request_id
log_app.insert(10,"request_id",log_app[9].apply(lambda x:x.split()[0].strip("[]")),True)



#get web dyno entries that received a request
contain_url=log_app[log_app[9].str.contains("path")].reset_index()

# create df
message_app=contain_url[9].apply(lambda x:x[x.find("method"):x.find("params")]).str.split().tolist()


temp_list = []
for m in message_app:
    temp_list.append([li for li in m if ("path" in li) | ("status" in li)])

app_url_DF=pd.DataFrame(temp_list)
app_url_DF.insert(0,"request_id",contain_url["request_id"],True)

log_router=log_DF[log_DF[8].str.contains("heroku/router")]
log_router_msg=log_router[9].tolist()

r_logs_message=[shlex.split(s) for s in log_router_msg]

# create df
final_list=[]
for s in r_logs_message:
    rlogs_dict2 = {}
    for i in s:
        rlogs_dict2[i.split("=")[0]] = i.split("=")[1]

    final_list.append(rlogs_dict2)

# df holding info for logs with source: heroku/router
DF_router=pd.DataFrame(final_list)
#keep only the rows with status=404
DF_router_status=DF_router.loc[DF_router["status"]=="404"]
# get the not found urls and the number of times they weren't found in the router logs
not_found_urls_r=DF_router_status.groupby(["path","host"]).size().reset_index(name='counts')
# count all urls in DF_router
count_all_urls=DF_router.groupby(["path","host"]).size().reset_index(name='counts')
#redirection is taking place
redirection=DF_router.loc[DF_router["status"]=="302"]

#keep only the rows with status=404
app_url_DF_n=app_url_DF.loc[app_url_DF[1]=="status=404"]
# drop duplicates on app log
app_url_DF_unique=app_url_DF_n.drop_duplicates(subset=["request_id"])
# get urls that don't exist in router logs
not_common=app_url_DF_unique.loc[~app_url_DF_unique["request_id"].isin(DF_router_status["request_id"])]
# get the not found urls and the number of times they weren't found in the app logs
not_found_urls_a=not_common.groupby([0]).size().reset_index(name='counts')

#average time to serve a page
avg_service=DF_router["service"].str.strip("ms").astype(int).mean()

#detect errors
server_error=DF_router.loc[DF_router["status"].str.contains("50")]
#server_error2=app_url_DF.loc[app_url_DF[1].str.contains("status=50")]
#error=log_DF.loc[log_DF[9].str.contains("error")]

# find which table is queried more often
insert_statements=log_DF[log_DF[9].str.contains("INSERT")]
tbl_insert=insert_statements[9].str.split("INTO").str[1].str.split().str[0]
select_statements=log_DF[log_DF[9].str.contains("SELECT")]
tbl_select=select_statements[9].str.split("FROM").str[1].str.split().str[0]
join_statements=log_DF[log_DF[9].str.contains("JOIN")]
tbl_join=join_statements[9].str.split("JOIN").str[1].str.split().str[0]
update_statements=log_DF[log_DF[9].str.contains("UPDATE")]
tbl_update=update_statements[9].str.split("UPDATE").str[1].str.split().str[0]
delete_statements=log_DF[log_DF[9].str.contains("DELETE")]
tbl_delete=delete_statements[9].str.split("FROM").str[1].str.split().str[0]
tables_DF=pd.DataFrame({'select': tbl_select.value_counts(), 'insert': tbl_insert.value_counts(),
                        'join': tbl_join.value_counts(), 'delete': tbl_delete.value_counts(),
                        'update': tbl_update.value_counts()}).fillna(0).astype(int)

tables_DF_sum=tables_DF.sum(axis=1).reset_index(name="sum")
sorted_DF=tables_DF_sum.sort_values(by=["sum"],ascending=False)