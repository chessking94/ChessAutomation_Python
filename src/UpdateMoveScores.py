import logging

import pyodbc

from base import base


class UpdateMoveScores(base):
    abbreviation = 'MOVESCORES'

    def _go(self):
        """ This process identifies and updates the move scores for any unanalyzed game imports """
        conn_str = self.config.get('connectionString')
        with pyodbc.connect(conn_str) as conn:
            sql_cmd = '''
SELECT fh.FileID

FROM ChessWarehouse.lake.Games AS g
INNER JOIN ChessWarehouse.dbo.FileHistory AS fh ON g.FileID = fh.FileID
    AND fh.DateCompleted IS NOT NULL
    AND fh.Errors = 0
LEFT JOIN (SELECT DISTINCT GameID FROM ChessWarehouse.lake.Moves WHERE TraceKey IS NULL) AS m ON g.GameID = m.GameID

WHERE fh.FileTypeID = 3

GROUP BY fh.FileID

HAVING COUNT(g.GameID) = SUM(CASE WHEN g.AnalysisStatusID = 3 THEN 1 ELSE 0 END)  --all games were analyzed successfully
AND COUNT(g.GameID) = SUM(CASE WHEN m.GameID IS NOT NULL THEN 1 ELSE 0 END)  --all games have yet to be scored
'''
        with conn.cursor() as csr:
            csr.execute(sql_cmd)
            fileids = [row[0] for row in csr.fetchall()]

            for f in fileids:
                if self.test_mode:
                    logging.info(f'FileID = {f} pending move scoring')
                else:
                    logging.info(f'Started updating move scores for FileID = {f}')
                    csr.execute(f'EXEC ChessWarehouse.dbo.UpdateMoveScores @fileid = {f}')
                    csr.commit()
                    logging.info(f'Ended updating move scores for FileID = {f}')
