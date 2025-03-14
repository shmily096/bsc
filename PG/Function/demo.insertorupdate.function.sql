-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION demo_merge(new_id integer, new_name TEXT, new_age integer) 
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE demo SET name = new_name, age = new_age WHERE demo.id = new_id;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO demo(id, name, age) VALUES (new_id, new_name, new_age);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;