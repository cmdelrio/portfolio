/** The following CTEs are the basis of a Periscope dashboard meant to report the results, successes,
and areas of improvement for a ThruTalk call campaign during the 2020 presidential election. The
CTEs beginning at line 500 return various counts/rates (e.g.positive ID rate) and 
tables (e.g. number of calls by state) used to build all of the different charts.**/

-- Duplicate records in results neccessitates MAX and GROUP BY statements
WITH results AS 
(
  SELECT 
  	id
  	,MAX(voter_phone) AS voter_phone
  	,MAX(voter_id) AS voter_id
  	,MAX(date_called) AS date_called
  	,MAX(service_account) AS service_account
  	,MAX(caller_login) AS caller_login
  	,MAX(result) AS result
  FROM tmc_thrutalk.sun_call_results
  GROUP BY 1
),

-- Get script responses to first candidate ID question
scale1 AS 
(
  SELECT 
    call_result_id
    ,MAX(answer) AS answer 
  FROM tmc_thrutalk.sun_script_results 
  WHERE question IN
  (
    'trump_to_biden_scale',
    'trump_to_biden_scale_checkbox_value_1',
    'trump_to_biden_scale_checkbox_value_2',
    'trump_to_biden_scale_checkbox_value_3',
    'trump_to_biden_scale_checkbox_value_4',
    'trump_to_biden_scale_checkbox_value_5',
    'trump_to_biden_scale_checkbox_value_6',
    'trump_to_biden_scale_checkbox_value_7',
    'trump_to_biden_scale_checkbox_value_8',
    'trump_to_biden_scale_checkbox_value_9'
  )
  GROUP BY 1
),

-- Ballot ready question was where callers indicated whether or not they were able to walk voter through
-- making a vote plan using Ballot ready
ballot_ready AS 
(
  SELECT 
    call_result_id
    ,MAX(answer) AS answer 
  FROM tmc_thrutalk.sun_script_results 
  WHERE question LIKE 'ballot_ready'
  GROUP BY 1
),
  
-- Get answers to the vote tripple question where people provide 3 names of friends. If not null, then 
-- they provided 3 names for vote trippling. 
votetrip AS 
(
  SELECT
    call_result_id
    ,MAX(answer) AS answer 
  FROM tmc_thrutalk.sun_script_results 
  WHERE question ILIKE 'if_yes_to_vt_3_friends_names'
  GROUP BY 1
),
  
-- Get script responses for start question. This is to make sure we exclude everyone who has gotten a 
-- call, wrong numbers, refused, ect. We need this because sometimes callers enter call results wrong.
results1 AS 
(
  SELECT
    call_result_id
    ,max(answer) AS answer 
  FROM tmc_thrutalk.sun_script_results
  WHERE 
  	answer ilike '%wrong%'
  	OR answer ILIKE '%moved%'
  	OR answer ILIKE '%talking%'
    OR answer ILIKE '%refused%'
    OR answer ILIKE '%deceased%' 
    OR answer ILIKE '%disconnected%' 
    OR answer ILIKE '%spanish%'
  GROUP BY 1
),

--Bring it all together in the final base from which we will pull call metrics from 
final_base AS 
(
  SELECT
  	results.id AS id
    ,results.voter_id AS voter_id
 	  ,results.date_called AS date_called
  	,results.service_account AS service_account
  	,results.caller_login AS caller_login
  	,results.result AS result
  	,scale1.answer AS trump_to_biden
  	,ballot_ready.answer AS ballot_ready
  	,votetrip.answer AS votetrip
  	,results1.answer AS first_question
  FROM results
  LEFT JOIN scale1 ON scale1.call_result_id = results.id
  LEFT JOIN ballot_ready ON ballot_ready.call_result_id = results.id
  LEFT JOIN votetrip ON votetrip.call_result_id = results.id
  LEFT JOIN results1 ON results1.call_result_id = results.id
),

-- Get a variety of call metrics
call_metrics as 
(
SELECT
	service_account 
  ,date_called 
-- assign a state based on call line and date. Getting this from the schedule. 
  ,CASE
    WHEN date_called LIKE '2020-10-02' AND service_account ilike '%1%' THEN 'MI'
  	WHEN date_called LIKE '2020-10-05' AND voter_id ILIKE '%mi%' THEN 'MI'
    WHEN date_called LIKE '2020-10-05' AND voter_id ILIKE '%az%' THEN 'AZ'
  	WHEN date_called LIKE '2020-10-06' AND service_account ilike '%1%' THEN 'FL'
  	WHEN date_called LIKE '2020-10-09' AND service_account ilike '%1%' THEN 'WI'
  	WHEN date_called LIKE '2020-10-09' AND service_account ilike '%2%' THEN 'AZ'
  	WHEN date_called LIKE '2020-10-11' AND service_account ilike '%2%' THEN 'AZ'
  	WHEN date_called LIKE '2020-10-13' AND service_account ilike '%1%' THEN 'WI'
		WHEN date_called LIKE '2020-10-13' AND service_account ilike '%2%' THEN 'AZ'
		WHEN date_called LIKE '2020-10-13' AND service_account ilike '%2%' THEN 'AZ'
		WHEN date_called LIKE '2020-10-16' AND service_account ilike '%1%' THEN 'MI'
  	WHEN date_called LIKE '2020-10-16' AND service_account ilike '%2%' THEN 'AZ'
  	WHEN date_called LIKE '2020-10-18' AND service_account ilike '%1%' THEN 'PA'
  	WHEN date_called LIKE '2020-10-18' AND service_account ilike '%2%' THEN 'FL'
  	WHEN date_called LIKE '2020-10-19' AND service_account ilike '%1%' THEN 'PA'
		WHEN date_called LIKE '2020-10-19' AND service_account ilike '%2%' THEN 'PA'
  	WHEN date_called LIKE '2020-10-20' AND service_account ilike '%1%' THEN 'PA'
  	WHEN date_called LIKE '2020-10-20' AND service_account ilike '%2%' THEN 'WI'
  	WHEN date_called LIKE '2020-10-23' AND service_account ilike '%1%' THEN 'AZ'
  	WHEN date_called LIKE '2020-10-23' AND service_account ilike '%2%' THEN 'WI'
  	ELSE 'TBD'
  END AS state

-- get dial count
    ,COUNT (*) AS dials

-- count connected to correct person
    ,SUM
    (
      CASE 
    	  WHEN first_question ILIKE '%Talking to Correct Person%' THEN 1 
    	  ELSE 0 
      END
      ) AS talking_to_correct_person_total 
        
-- count wrong numbers
	  ,SUM
     (
      CASE 
   		  WHEN first_question ILIKE '%wrong%' THEN 1 
    	  ELSE 0 
      END
      ) AS wrong_number_total
        
-- count refused (jerks)
    ,SUM
    (
      CASE 
   		  WHEN first_question ILIKE '%refused%' THEN 1 
    	  ELSE 0 
      END
     ) AS refused_total
        
-- count ids     
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden IS NOT NULL THEN 1
      	ELSE 0 
      END
     ) AS ids
        
-- count positive ids            
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden IN (6,7,8,9,10) THEN 1
       	ELSE 0 
      END
    ) AS positive_ids
        
-- count the number of people who went through ballot ready with a phonebanker
	  ,SUM
    (
      CASE 
      	WHEN ballot_ready IN ('complete', 'unregistered_wi','unregistered_mi','unregistered_pa','unregistered_nc','unregistered_az','unregistered_fl') THEN 1
       	ELSE 0 
      END
    ) AS completed_ballot_ready
        
-- count the number of people who gave the names of 3 friends
	  ,SUM
    (
      CASE 
      	WHEN votetrip IS NOT NULL THEN 1
       	ELSE 0 
      END) AS vote_triple_total

-- the following 10 case whens count the number of instances of each ID rank
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden =1 THEN 1
       	ELSE 0 
      END
      ) AS "1s"
	
      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =2 THEN 1
       	ELSE 0 
      END
      ) AS "2s"

      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =3 THEN 1
       	ELSE 0 
      END
      ) AS "3s"
	
      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =4 THEN 1
       	ELSE 0 
      END
      ) AS "4s"
	
      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =5 THEN 1
       	ELSE 0 
      END
      ) AS "5s"
        
      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =6 THEN 1
       	ELSE 0 
      END
      ) AS "6s"

      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =7 THEN 1
       	ELSE 0 
      END
      ) AS "7s"

      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =8 THEN 1
       	ELSE 0 
      END
      ) AS "8s"

      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =9 THEN 1
       	ELSE 0 
      END
      ) AS "9s"
        
      ,SUM
      (
      CASE 
      	WHEN trump_to_biden =10 THEN 1
       	ELSE 0 
      END
      ) AS "10s"

-- count not voting ppl (sad)
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden LIKE 'not_voting' THEN 1
       	ELSE 0 
      END
      ) AS not_voting

--  count people who already voted
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden LIKE 'already_voted' THEN 1
       	ELSE 0 
      END
      ) AS already_voted

-- count people who are ineligible to vote
	  ,SUM
    (
      CASE 
      	WHEN trump_to_biden LIKE 'ineligible' THEN 1
       	ELSE 0 
      END
     ) AS ineligible
  	
-- the next section is to get rates
-- rate of connected to correct voter
    ,SUM
    (
    	CASE
			  WHEN first_question ilike '%Talking to Correct Person%' THEN 1 
    		ELSE 0 
      END
      )::DECIMAL 
     /
     COUNT (*)::DECIMAL 
     AS talked_to_correct_voter_rate

-- wrong number rate
    ,SUM
    (
    	CASE
			  WHEN first_question ilike '%wrong%' THEN 1 
    		ELSE 0 
      END
      )::DECIMAL 
     /
     COUNT (*)::DECIMAL 
     AS wrong_number_rate

-- refused rate
	` ,SUM
    (  
      CASE
			  WHEN first_question ilike '%refused%' THEN 1 
    		ELSE 0 
      END
      )::DECIMAL 
     /
     COUNT (*)::DECIMAL 
     AS refused_rate
      
-- ID rate
	  ,SUM
    (
		  CASE 
      		WHEN trump_to_biden IS NOT null THEN 1
      		ELSE 0 
      END
      )::DECIMAL 
     /
     COUNT (*)::DECIMAL 
     AS id_rate

-- positive ID rate
	  ,SUM
    (
      	CASE 
      		WHEN trump_to_biden IN (7,8,9,10) THEN 1
       		ELSE 0 
      	END
      )::DECIMAL
     /
	  NULLIF
    (
        SUM
        (
          CASE 
            WHEN trump_to_biden IN (1,2,3,4,5,6,7,8,9,10) THEN 1 
            ELSE 0 
           END
         ):: DECIMAL
		 ,0
     ) AS positive_id_rate

-- Undecided rate
	  ,SUM
    (
      	CASE 
      		WHEN trump_to_biden = 5 then 1
       		else 0 
      		end):: decimal
     /
	  NULLIF
    (
        SUM
        (
          CASE 
            WHEN trump_to_biden IN (1,2,3,4,5,6,7,8,9,10) THEN 1 
            ELSE 0 
           END
         ):: DECIMAL
		 ,0
     ) AS undecided_rate

-- not voting rate
	  ,SUM
    (
      	CASE 
      		WHEN trump_to_biden LIKE 'not_voting' THEN 1
       		ELSE 0 
      	END
    	)::DECIMAL
     /
	  NULLIF
    (
        SUM
        (
          CASE 
            WHEN first_question ILIKE '%Talking to Correct Person%' THEN 1 
    			  ELSE 0 
           END
         ):: DECIMAL
		 ,0
     ) AS not_voting_rate

-- ballot ready completed rate
	  ,SUM
    (
      	CASE 
      		WHEN ballot_ready IN ('complete', 'unregistered_wi','unregistered_mi','unregistered_pa','unregistered_nc','unregistered_az','unregistered_fl') THEN 1
       		ELSE 0 
      	END):: DECIMAL
     /
	  NULLIF
    (
        SUM
        (
          CASE 
            WHEN first_question ILIKE '%Talking to Correct Person%' THEN 1 
    		    ELSE 0 
         ):: DECIMAL
		 ,0
     ) AS completed_ballot_ready_rate,

-- vote tripple rate
	  ,SUM
    (
      	CASE 
      		WHEN votetrip IS NOT NULL THEN 1.0
          ELSE 0 
      	 END
    	)::DECIMAL
	/
     /
	  NULLIF
    (
        SUM
        (
          CASE 
            WHEN first_question ILIKE '%Talking to Correct Person%' THEN 1 
    		    ELSE 0 
         ):: DECIMAL
		 ,0
     ) AS vote_triple_rate**/
FROM final_base
-- Exclude phonebanks that shouldn't be reported in this dashboard
WHERE NOT 
(voter_id ILIKE '%tx%' AND service_account ILIKE '%2%')
GROUP BY date_called AND state
),

-- Get caller data and group by email so that our averages make sense and are on a per caller basis not 
-- a per login basis
callers AS 
(
  SELECT 
    email
  	,UPPER(MAX(name)) as name
  	,date
  	,service_account
  	,SUM(minutes_in_ready) AS minutes_in_ready
  	,SUM(no_contact) + SUM(remove_number_from_list) + SUM(talked_to_correct_person) AS total_dials
  FROM tmc_thrutalk.sun_callers
  GROUP BY email, date, service_account
),

-- Now do some calculations so that this is ready to join with our call result metrics
caller_metrics AS
(
  SELECT
      callers.date
      ,callers.service_account
  	  ,COUNT(distinct(callers.email)) AS total_callers,
      ,AVG(callers.minutes_in_ready) AS avg_minutes_in_ready,
      ,AVG(callers.total_dials) AS avg_dials_per_caller
  FROM callers
  GROUP BY 1,2
),


-- Get table with different call metrics (total positive ids, total convos, total vote tripples, 
-- positive id rate, ect) for the whole program
overall_program_results AS
(
  SELECT
    *
  FROM call_metrics
),

--Get table with call results for each individual phonebank
results_by_phonebank AS
(
  SELECT
    *
  FROM call_metrics
  GROUP BY date_called AND state
),

--Get table with number of conversations per state
results_by_state AS
(
  SELECT
    talking_to_correct_person_total
  FROM call_metrics
  GROUP BY state
),

-- Get total call shifts per date
callshifts_by_date AS
(
  SELECT
    date
    COUNT(*)
  FROM callers
  GROUP BY date
),

-- Get top callers
top_callers AS
(
  SELECT
    MAX(name)
    ,email
    ,COUNT(*) AS total_shifts
    ,SUM(minutes_in_ready) AS minutes_in_ready
    ,SUM(total_dials) AS total_dials
  FROM callers
  GROUP BY email
),

-- Get table counting number of shifts attended and the number of callers who fall into each bucket
returner_rate AS
(
  SELECT
    shifts_attended
    ,COUNT(*)
  FROM (
        SELECT
          email
          ,COUNT(*) as shifts_attended
        FROM callers
        GROUP BY 1)
  GROUP BY 1
  ORDER BY 1 
),

-- Get avg minutes in ready for each phonebank
minutes_in_ready_by_phonebank AS
(
  SELECT 
    date
    ,AVG(minutes_in_ready) AS avg_minutes_in_ready
  FROM callers
  GROUP BY 1
),

-- Get rate at which callers get a second ID based on the first ID they got (is persuation working)
-- or are callers just getting hung up on?
persuasion_effectiveness AS
(
  SELECT 
    q1.answer AS "initial rating"
    ,COUNT(DISTINCT(q1.call_result_id)) AS "total ids"
    ,1.0 - 
      (COUNT(DISTINCT(q1.call_result_id) - (COUNT(DISTINCT(q2.call_result_id)))::DECIMAL 
      / 
      COUNT(DISTINCT(q1.call_result_id))::DECIMAL) 
    AS "caller gets second id"
    ,SUM(q2.answer::DECIMAL)::DECIMAL/COUNT(q2.answer)::DECIMAL
      - q1.answer::DECIMAL 
    AS "avg ID shift"
  FROM tmc_thrutalk.sun_script_results AS q1
  LEFT JOIN (
            SELECT * 
            FROM tmc_thrutalk.sun_script_results 
            WHERE question ilike 'final_rating'
            ) AS q2
  ON q1.call_result_id = q2.call_result_id
  WHERE q1.question ILIKE 'trump_to_biden_scale' and q1.answer IN (2,3,4,5)
  GROUP BY 1
  ORDER BY 1
)

SELECT ___ FROM ____
