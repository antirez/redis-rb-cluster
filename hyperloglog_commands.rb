module HyperLogLogCommands
    def pfadd(key, *elements)
        send_cluster_command([:pfadd, key, elements])
    end

    def pfcount(*keys)
        keys.uniq.reduce(0) {|agg, key|
            agg += send_cluster_command([:pfcount, key])
        }
    end

    def pfmerge(destkey, *sourcekeys)
        # HACK: since pfmerge is generally performed with a single source key,
        # using the big HACK to use demp/restore instead of duplicating HLL
        # merge in the client should suffice
        if sourcekeys.length > 0
            raise 'cluster pfmerge currently only supports a single source key'
        end
        sourcekeys.each {|sourcekey|
            bytes = send_cluster_command([:dump, sourcekey])
            send_cluster_command([:restore, destkey, 0, bytes])
        }
        'OK'
    end
end
