# Read csv file and Change null To NA 
d=read.csv("/home/vaibhav/Downloads/click_process/click_process (copy).csv",sep="|",header=TRUE,na.strings = c('NA','null',""))

# Removing Rows with NA value And Reassigning it to d 
d=d[complete.cases(d),]

region_no =c(0,1,2,3,15,27,40,55,65,79,80,94,106,124,134,146,164,183,201,216,238,253,275,276,298,308,325,333,344,359,368,374,393,394,395)
region_name = c("unknown","beijing","ianjin","hebei","shanxi","neimenggu","liaoning","jilin","heilongjiang","shanghai","jiangsu","zhejiang","anhui","fujian","jiangxi","shandong","henan","hubei","hunan","guangdong","guangxi","hainan","chongqing","sichuan","guizhou","yunnan","xizang","shannxi","gansu","qinghai","ningxia","xinjiang","taiwan","xianggang","aomen") 

user_tags_no=c(10006,10024,10031,10048,10052,10057,10059,10063,10067,10074,10075,10076,10077,10079,10083,10093,10102,10684,11092,11278,11379,11423,11512,11576,11632,11680,11724,11944,13042,13403,13496,13678,13776,13800,13866,13874,14273,16593,16617,16661,16706,16751,10110,10111)
user_tags_name=c("news","eduation","automobile","real estate","IT","electronic game","fashion","entertainment","luxury","home and lifestyle","health","food","divine","motherhood&parenting","sports","travel&outdoors","social","3c product","appliances","clothing、shoes&bags","Beauty& Personal Care","household&home improvement","infant&mom products","sports item","outdoor","health care products","luxury","real estate","automobile","finance","travel","education","service","art&photography&design","online literature","electronic game","3c","book","medicine","food&drink","culture","sex","male","female")

# Remove bid_is,user_agent amd log_type coloumns and Reassign it to data variable
data_k=d[,-c(1,2,3,7)]
data=d[,-c(1,2,3,5,6)]
#data

#order by Region
data_k=data_k[order(data[,1]),]
data=data[order(data[,1]),]
#data

# Find all the unique values in an coloumn
uni=rapply(data_k,function(x)length(unique(x)))
uni
#Assigning no of unique in region to cluster size
cs=uni[1]
cs

result=kmeans (data_k,cs)
clus<-result$cluster
data_k = data.frame(data_k,clus)

t=table(data_k$region,result$cluster)
t= as.data.frame.matrix(t)

threshold = ( nrow(data_k)/cs)/2;

for (i in 1:nrow(data_k))
{
   cn = data_k$clus[i]
   rg = data_k$region[i]
   rs=as.character(rg)
   p=as.numeric( t[rs,cn])
   #cat('\n',"I ", i, "cn ", cn,"rg ",rg,"rs ",rs,"p ",p)
   if(!is.na(rs))
   if(p<threshold)
   {
           data_k=data_k[-c(i),]
           data=data[-c(i),]
   }        
}


data$user_tags=as.character(data$user_tags)
user_tags_n=vector(mode="character",length=nrow(data))


for (i in 1:nrow(data))
{
        v=as.numeric(unlist(strsplit(data[i,2],",")))
        v=sort(v)
        v=as.character(v)
        for (k in 1:length(v))
                for (j in 1:length(user_tags_name))
                {
                        if(v[k]==user_tags_no[j]){
                                v[k]=user_tags_name[j]
                                break
                        }          
                } 
        v=paste(v,collapse=" ")
        user_tags_n[i]=v      
}

data = data.frame(data,user_tags_n)
#data_k = data.frame(data_k,user_tags_n)
data = data.frame(data,data_k$clus)
data = data[,-2]
#data_k = data_k[,-4]

data$region=as.character(data$region)

for (i in 1:nrow(data))
        for (j in 1:length(region_name))
        {
                if(data$region[i]==region_no[j]){
                        data$region[i]=region_name[j]
                        break
                }          
        }   

vec_r = data[,1]
vec_r = unique(vec_r)

vec_u = data[,2]
vec_u = unique(vec_u)

vec_c = data[,3]
vec_c = unique(vec_c)

library(RNeo4j)

graph = startGraph("http://localhost:7474/db/data/")


clear(graph)

addConstraint(graph,"Cluster","name")

for (i in 1:length(vec_c)){
        ch=as.character(vec_c[i])
        createNode(graph,"Cluster",name =ch)
}

addConstraint(graph,"Region","name")

for (i in 1:length(vec_r)){
        ch=as.character(vec_r[i])
        createNode(graph,"Region",name =ch)
}

addConstraint(graph,"User_Tags","name")
for (i in 1:length(vec_u)){
        ch=as.character(vec_u[i])
        createNode(graph,"User_Tags",name =ch)
}
id = c(30:40,50:60,100:110,150:160,200:210)
for(i in id){
        tmp1="MATCH (r : Region  {name: '"
        tmp2=as.character(data$region[i])
        tmp3="'}) RETURN r"
        tmp = c(tmp1,tmp2,tmp3)
        tmp = paste(tmp, collapse="")
        
        
        
        r = getNodes(graph,tmp)[[1]]                                                                                                                                                                                                                                                                                                                                                                                                        
        
        tmp1="MATCH (r : Cluster  {name: '"
        tmp2=as.character(data$data_k.clus[i])
        tmp3="'}) RETURN r"
        tmp = c(tmp1,tmp2,tmp3)
        tmp = paste(tmp, collapse="")
        
        c = getNodes(graph,tmp)[[1]]
        
        createRel(r,"Lies_In",c,color="Red")
}

for(i in id){
        tmp1="MATCH (r : Region  {name: '"
        tmp2=as.character(data$region[i])
        tmp3="'}) RETURN r"
        tmp = c(tmp1,tmp2,tmp3)
        tmp = paste(tmp, collapse="")
        
        
        
        r = getNodes(graph,tmp)[[1]]                                                                                                                                                                                                                                                                                                                                                                                                        
        
        tmp1="MATCH (r : User_Tags {name: '"
        tmp2=as.character(data$user_tags_n[i])
        tmp3="'}) RETURN r"
        tmp = c(tmp1,tmp2,tmp3)
        tmp = paste(tmp, collapse="")
        
        c = getNodes(graph,tmp)[[1]]
        
        createRel(r,"Has_Tag",c,color="Red")
}
#getConstraint(graph)

browse(graph)

