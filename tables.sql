idl_limao_cid_dim
----------------------------------
root_cid            	string              	                    
root_name           	string              	                    
subroot_cid         	string              	                    
subroot_name        	string              	                    
cid                 	string              	                    
name                	string              	                    
parent_cid          	string              	                    
title_set           	array<string>       	                    
ds                  	string     

config_newcid_dim
-----------------------------------
root_name           	string              	                    
subroot_name        	string              	                    
newroot             	string              	                    
newsubroot          	string              	                    
ds                  	string              	                    
	 	 

SELECT COUNT(DISTINCT newroot) --26
FROM config_newcid_dim;
SELECT COUNT(DISTINCT newsubroot) --85
FROM config_newcid_dim;

SELECT COUNT(DISTINCT root_name) --26
FROM tmp_kgs_newrootsubroot_title_set;
SELECT COUNT(DISTINCT subroot_name) --85
FROM tmp_kgs_newrootsubroot_title_set;

-- root_name:122
-- subroot_name:1480
SELECT COUNT(DISTINCT subroot_name)
FROM idl_limao_cid_dim
WHERE ds='2017-06-26'
AND root_name IS NOT NULL;
