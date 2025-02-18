-- query 1
CREATE TABLE query1 AS SELECT COUNT(*) AS "count of comments" FROM comments WHERE author = 'xymemez';

-- query 2
CREATE TABLE query2 AS SELECT subreddit_type AS "subreddit type", COUNT(name) AS "subreddit count" FROM subreddit GROUP BY subreddit_type;

--query 3
CREATE TABLE query3 AS SELECT subreddit AS "name", COUNT(*) AS "comments count", ROUND(AVG(score), 2) AS "average score" FROM comments GROUP BY subreddit ORDER BY  "comments count" DESC LIMIT 10;

--query 4
CREATE TABLE query4 AS SELECT name AS "name", link_karma AS "link karma", comment_karma AS "comment karma", CASE WHEN link_karma >= comment_karma THEN 1 ELSE 0 END AS "label" FROM author WHERE (link_karma + comment_karma)/2 > 1000000 ORDER BY (link_karma + comment_karma) / 2 DESC;

--query 5
CREATE TABLE query5 AS SELECT subr.subreddit_type AS "sr type", COUNT(comnt.id) AS "comments num" FROM comments comnt JOIN subreddit subr ON comnt.subreddit_id = subr.name WHERE comnt.author = '[deleted_user]' GROUP BY subr.subreddit_type;