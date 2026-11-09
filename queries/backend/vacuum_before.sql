DELETE FROM
    jobs
WHERE
    (
        status = 'Done'
        OR status = 'Killed'
        OR (
            status = 'Failed'
            AND max_attempts <= attempts
        )
    )
    AND done_at <= TIMESTAMPADD(SECOND, ?, NOW());
