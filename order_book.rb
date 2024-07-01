# app/models/order_book.rb

class OrderBook < ActiveRecord::Base
  # An ExchangeMarket is a market where orders of a particular curreny pair are matched.
  # A exchange platform usually have many markets (BTCEUR, ETHEUR, BTCUSD, etc)
  belongs_to :exchange_market

  # It is the market that defines the curreny pair, we do need it here in the Orderbook too
  delegate :currency_pair, to: :exchange_market

  # Saves the bids/asks entries as JSON in the datastore.
  serialize :asks, JSON
  serialize :bids, JSON

  def asks
    objects = arrays_to_objects(self[:asks])
    objects.sort { |x, y| x.price <=> y.price }
  end

  def bids
    objects = arrays_to_objects(self[:bids])
    objects.sort { |x, y| y.price <=> x.price }
  end

  # When calculating the potentiel profit, we don't want to take into account the volumes or prices of our own
  # orders present in the order book.
  # Returns the best ask orders that are not ours.
  def best_stranger_asks
    active_entries = exchange_market.orders.asks.active
    stranger_entries(asks, active_entries)
  end

  # Returns the best bid orders that are not ours.
  def best_stranger_bids
    active_entries = exchange_market.orders.bids.active
    stranger_entries(bids, active_entries)
  end

  # Returns the best ask order that is not ours.
  def best_stranger_ask
    best_stranger_asks.first
  end

  # Returns the best bid order that is not ours.
  def best_stranger_bid
    best_stranger_bids.first
  end

  # Returns the best ask order, whether it is ours or not.
  def best_ask
    asks.first
  end

  # Returns the best bid order, whether it is ours or not.
  def best_bid
    bids.first
  end

  # Each entry (i.e. order) in the orderbook can be updated remotely on the exchange platform. When this happens,
  # we need to update our local copy ot the orderbook.
  # A volume set to zero usually means that the entry was removed (order canceled or fully matched).
  # A volume update usually means that the entry was partially matched.
  def update_with_entry!(action:, entry:)
    entry.volume.zero? ? remove_entry!(action, entry) : add_or_update_entry!(action, entry)
  end

  def to_s
    "#{exchange_market} order book"
  end

  private

  # Remove a bid/ask from the orderbook
  def remove_entry!(action, entry)
    self[action] = send(action).delete_if { |e| e.price == entry.price && e.timestamp > entry.timestamp }.map(&:to_a)
    self.data_updated_at = Time.zone.now
    save!
  end

  def has_entry?(action, entry)
    send(action).detect { |e| e.price == entry.price }
  end

  # Update a bid/ask entry in the orderbook
  def update_entry!(action, entry)
    self[action] = send(action).each do |e|
      if e.price == entry.price
        e.volume = entry.volume
        e.timestamp = entry.timestamp
      end
    end.map(&:to_a)

    self.data_updated_at = Time.zone.now
    save!
  end

  # Add a bid/ask entry to the orderbook
  def add_entry!(action, entry)
    self[action] << entry.to_a
    self[action] = format_entries(action, self[action])
    self.data_updated_at = Time.zone.now
    save!
  end

  # Add or update a bid/ask entry in the orderbook
  def add_or_update_entry!(action, entry)
    if has_entry?(action, entry)
      update_entry!(action, entry)
    else
      add_entry!(action, entry)
    end
  end

  # Instanciate a OrderBookEntry object for each entry.
  def arrays_to_objects(arrays)
    arrays.map do |entry|
      OrderBookEntry.new(price: entry[0], volume: entry[1], timestamp: entry[2])
    end
  end

  # Some exchanges do not provide any information on wether an order is ours or not.
  # In addition, similar orders to ours (same prices and volume) can exist and some exchanges do not provide any
  # information on this either. So we need to keep track of everything and remove our orders and/or volumes
  # to have workable data.
  def stranger_entries(all_entries, own_entries)
    return all_entries if own_entries.empty?

    all_entries.map do |entry|
      if same_price_entry = own_entries.find { |e| entry.price == e.price }
        entry.volume -= same_price_entry.volume
        entry.volume.zero? ? nil : entry
      else
        entry
      end
    end.compact
  end

  # We need to sort the entries in the different order depending on the type (bid or ask)
  # So as we always get the best entry *first* in the orderbook
  def format_entries(type, entries)
    raise ArgumentError unless %i[asks bids].include?(type)

    sort_block = if type == :asks
      lambda { |a, b| a[0] <=> b[0] }
    else
      lambda { |a, b| b[0] <=> a[0] }
    end

    entries.sort(&sort_block)
  end
end
