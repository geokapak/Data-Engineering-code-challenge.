-- Create tables

CREATE SCHEMA project_test;
USE project_test;
CREATE TABLE new_deal(
    id INTEGER NOT NULL,
    pipeline_id INTEGER,
    user_id VARCHAR(50),
    status_ VARCHAR(50),
    value_ DECIMAL(15,2),
    Currency VARCHAR(50),
    total_activities INTEGER,
    active_status VARCHAR(50),
    primary key (id)
);

CREATE TABLE new_activities(
			activity_id INTEGER NOT NULL,
            deal_id INTEGER,
            `Type` VARCHAR(50),
            marked_as_done_ts DATE,
            deleted VARCHAR(50),
            num_false INTEGER,
            PRIMARY KEY (activity_id)
);
CREATE TABLE project_test.new_deals_updates(
			deal_id INTEGER,
            update_type VARCHAR(50),
            old_value VARCHAR(50),
            new_value VARCHAR(50)
            
);

