

CREATE OR REPLACE VIEW master.executive_report
AS
WITH all_count AS (
         SELECT to_char(all_dq_data.create_date, 'yyyy-mm-dd'::text) AS run_date,
            count(1) AS all_count
           FROM master.all_dq_data
          GROUP BY (to_char(all_dq_data.create_date, 'yyyy-mm-dd'::text))
        ), new_issues AS (
         SELECT sum(weekly_summary.new_issues_count) AS new_issues,
            weekly_summary.run_date
           FROM master.weekly_summary
          GROUP BY weekly_summary.run_date
        ), closed_issues AS (
         SELECT sum(weekly_summary.closed_issues_count) AS closed_issues,
            weekly_summary.run_date
           FROM master.weekly_summary
          GROUP BY weekly_summary.run_date
        ), avg_age AS (
         SELECT round(avg(to_number(all_issues_attribute.duration::text, '99'::text))) AS avg_age,
            to_char(all_issues_attribute.issue_end_dt, 'yyyy-mm-dd'::text) AS run_date
           FROM master.all_issues_attribute
          GROUP BY (to_char(all_issues_attribute.issue_end_dt, 'yyyy-mm-dd'::text))
        )
SELECT all_count.run_date,
    sum(all_count.all_count) OVER (ORDER BY all_count.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS all_issues,
    sum(new_issues.new_issues) OVER (ORDER BY new_issues.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS new_issues,
    sum(closed_issues.closed_issues) OVER (ORDER BY closed_issues.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS closed_issues,
    avg_age.avg_age
   FROM all_count
     LEFT JOIN closed_issues ON all_count.run_date = closed_issues.run_date
     LEFT JOIN new_issues ON all_count.run_date = new_issues.run_date
     LEFT JOIN avg_age ON all_count.run_date = avg_age.run_date
     where date_part('year', to_date(all_count.run_date,'yyyy')) = date_part('year', CURRENT_DATE) 
  ORDER BY all_count.run_date;


select * from master.executive_report;
