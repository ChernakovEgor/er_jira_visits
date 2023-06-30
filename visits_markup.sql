WITH hits AS (
			SELECT o."Time" AS hit_time,
	  			   o."ID" AS object_id,
		  		   o."ID" AS object_name,
			  	   o."Type" AS object_type,
				     o."Method" AS object_method,
				     o."System" AS object_system,
				     at2."Action" AS action_name,
				     at2."Area" AS area,
				     at2."Category" AS category,
				     a."ID" AS author_id,
				     a."Name" AS author_name,
				     a."Type" AS author_type,
				     a."URL" AS url
			  FROM splunk.object o
	INNER JOIN splunk.audit_type at2
	        ON o."Session_id" = at2."Session_id" 
	       AND o."Event_id" = at2."Event_id" 
	INNER JOIN splunk.author a 
	    		ON o."Session_id" = a."Session_id" 
	       AND o."Event_id" = a."Event_id" 
	     WHERE o."Time" > now() - INTERVAL '60 day'
	       AND a."Type" != 'user'
	       AND a."Type" != 'system'
    ORDER BY hit_time ASC
	           )
SELECT md5(visit_start || author_id) AS visit_id,
       hit_time, 
       EXTRACT(EPOCH FROM hit_time - visit_start)::NUMERIC(8,0) AS visit_duration,
       object_id, object_name, object_type, object_method, object_system, action_name, area, category, author_id, author_name, author_type, url,
       CASE WHEN author_id IN (SELECT author_id FROM splunk.dict_bot) THEN TRUE ELSE FALSE END is_bot, 
       1 AS session_id,
       NULL AS _deleted,
       date_trunc('day', hit_time)::date AS reporting_period,
       'ER-5' AS kpi_type
  FROM (
        SELECT *, 
             MIN(hit_time) OVER(PARTITION BY author_id, user_hits - flags) AS visit_start
          FROM (
              SELECT *,
                   SUM(CASE WHEN in_interval THEN 1 ELSE 0 END) OVER(PARTITION BY author_id ORDER BY user_hits ASC) AS flags
                FROM (
                   SELECT *,
                        hit_time - LAG(hit_time) OVER (ORDER BY author_id, hit_time ASC) < INTERVAL '30 min' AS in_interval,
                        ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY hit_time ASC) AS user_hits
                     FROM hits
                   ) AS step1
               ) AS step2
    		) AS step3
