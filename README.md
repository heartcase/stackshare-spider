# stackshare-spider
Python spider + sql learning project  


ALL DATA BELONG TO STACKSHARE.IO  
STUDY PURPOSE ONLY  

How to use this program:  
create db:  
run python new_db.py first  

run spider:  
from controller import controller  
from controller import task  

controller.update_all()  
task.get_queue().join()  
task.stop()  

