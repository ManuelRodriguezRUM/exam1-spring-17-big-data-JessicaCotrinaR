# You sql follows

Select region, ciudad, count(*)
from escuelaspr
group by region, ciudad;


select sc.identificacion, count(*)
from studentspr e, escuelaspr sc
where e.identificacion_escuela = sc.identificacion
group by sc.identificacion;


select identificacion_student, escuelaspr.nombre
from escuelaspr, studentspr
where escuelaspr.identificacion = studentspr.identificacion_escuela 
and (escuelaspr.ciudad = “Ponce” or escuelaspr.ciudad = “Cabo Rojo”);


select es.region, es.ciudad, count(*)
from studentspr st, escuelaspr es
where st.identificacion_escuela = es.identificacion
group by es.region, es.ciudad; 
