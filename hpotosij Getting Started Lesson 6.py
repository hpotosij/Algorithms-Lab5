# Import Algorithm API
import quantopian.algorithm as algo

# Pipeline imports
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.psychsignal import stocktwits
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.filters import QTradableStocksUS


def initialize(context):
    # Attach pipeline to algorithm
    algo.attach_pipeline(
        make_pipeline(),
        'data_pipe'
    )

    # Schedule rebalance function
    algo.schedule_function(
        rebalance,
        date_rule=algo.date_rules.week_start(),
        time_rule=algo.time_rules.market_open()
    )


def before_trading_start(context, data):
    # Get pipeline output and -- salida de pipeline 
    # store it in context -- y almacenar en context
    context.pipeline_data = algo.pipeline_output('data_pipe')


def rebalance(context, data):
    # Display first 10 rows -- mostrar filas de pipeline
    # of pipeline output
    log.info(context.pipeline_data.head(10))


# Pipeline definition
def make_pipeline():

    base_universe = QTradableStocksUS()

    sentiment_score = SimpleMovingAverage(
        inputs=[stocktwits.bull_minus_bear],
        window_length=3,
    )

    return Pipeline(
        columns={
            'sentiment_score': sentiment_score,
        },
        screen=(
            base_universe
            & sentiment_score.notnull()
        )
    )