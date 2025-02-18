-- Primary Key
-- Authors Table
ALTER TABLE author ADD CONSTRAINT author_pk_id PRIMARY KEY (id);
-- Subreddits Table
ALTER TABLE subreddit ADD CONSTRAINT subreddit_pk_id PRIMARY KEY (id);
-- Submissions Table
ALTER TABLE submissions ADD CONSTRAINT submissions_pk_id PRIMARY KEY (id);
-- Comments Table
ALTER TABLE comments ADD CONSTRAINT comments_pk_id PRIMARY KEY (id);


-- Unique Constraint
-- Authors Table 
ALTER TABLE author ADD CONSTRAINT author_unq_name UNIQUE (name);
-- Subreddits Table
ALTER TABLE subreddit ADD CONSTRAINT subreddit_unq_name UNIQUE (name);
ALTER TABLE subreddit ADD CONSTRAINT display_unq_name UNIQUE (display_name);


-- Foreign Key
-- Submissions Table
ALTER TABLE submissions ADD CONSTRAINT submissions_fk_author FOREIGN KEY (author) REFERENCES author(name);
ALTER TABLE submissions ADD CONSTRAINT submissions_fk_subreddit FOREIGN KEY (subreddit_id) REFERENCES subreddit(name);

-- Comments Table
ALTER TABLE comments ADD CONSTRAINT comments_fk_author FOREIGN KEY (author) REFERENCES author(name);
ALTER TABLE comments ADD CONSTRAINT comments_fk_subreddit_id FOREIGN KEY (subreddit_id) REFERENCES subreddit(name);
ALTER TABLE comments ADD CONSTRAINT comments_fk_subreddit FOREIGN KEY (subreddit) REFERENCES subreddit(display_name);