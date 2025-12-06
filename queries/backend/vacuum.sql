Delete from
    jobs
where
    status = 'Done'
    OR status = 'Killed'
    OR (
        status = 'Failed'
        AND max_attempts <= attempts
    );
