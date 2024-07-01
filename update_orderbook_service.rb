# app/services/update_orderbook_service.rb

class UpdateOrderbookService
  include Callable

  def initialize(message:, side:, exchange:, counterpart_exchange:, currency_pair:, store: $redis)
    @message = message
    @side = side
    @exchange = exchange
    @counterpart_exchange = counterpart_exchange
    @currency_pair = currency_pair
    @config = TRADERS_CONFIG[currency_pair.iso_code]['strategies']
    @store = store
  end

  def call
    load_orderbooks

    # If orderbooks are outdated, we don't want to execute any orders as this may result in losses
    return false if orderbooks_outdated?

    # We first try to execute the market taking strategy : it consists in checking whether the two orderbooks have a
    # spread wide enough for us to make a profit. If so, we execute the orders immediately and take the profit.
    # Otherwise, we go to the next strategy.
    return if execute_market_taking_strategy

    # If the market taking strategy wasn't possible, we execute the market making strategy : it consists in placing a
    # limit order and wait for a buyer/seller a place a matching order. The order stays in the orderbook until it
    # is matched or until a better order can be placed to replace it.
    return if execute_market_making_strategy
  end

  private

  def load_orderbooks
    @orderbook = Orderbook.new(@exchange, @currency_pair)
    @orderbook.fill_entries_with(@message['orderbook'])

    system_bids = Order.where(state: :executed, type: :limit, action: :buy).with_exchange(@exchange).map(&:to_hash)
    system_asks = Order.where(state: :executed, type: :limit, action: :sell).with_exchange(@exchange).map(&:to_hash)
    @orderbook.fill_system_entries_with({ bids: system_bids, asks: system_asks })

    @counterpart_orderbook = Orderbook.new(@counterpart_exchange, @currency_pair)

    counterpart_orderbook_entries = @store.with do |connection|
      connection.get("orderbooks:#{@counterpart_exchange}:#{@currency_pair.iso_code}")
    end

    @counterpart_orderbook.fill_entries_with(JSON.parse(counterpart_orderbook_entries))
  end

  def orderbooks_outdated?
    @orderbook.outdated? || @counterpart_orderbook.outdated?
  end

  def execute_market_taking_strategy
    Strategies::MarketTaking::UpdateOrderbookService.call(
      side: @side,
      volume: @config['market_taking']['volume'],
      minimum_profit_rate: @config['market_taking']['cutoff_rate'],
      currency_pair: @currency_pair,
      exchange: @exchange,
      counterpart_exchange: @counterpart_exchange,
      orderbook: @orderbook,
      counterpart_orderbook: @counterpart_orderbook
    )
  end

  def execute_market_making_strategy
    Strategies::MarketMaking::UpdateOrderbookService.call(
      side: @side,
      volume: @config['market_making']['volume'],
      minimum_profit_rate: @config['market_making']['cutoff_rate'],
      currency_pair: @currency_pair,
      exchange: @exchange,
      counterpart_exchange: @counterpart_exchange,
      orderbook: @orderbook,
      counterpart_orderbook: @counterpart_orderbook,
      bid_increment: @config['market_making']['bid_increment'],
      ask_decrement: @config['market_making']['ask_decrement']
    )
  end
end
