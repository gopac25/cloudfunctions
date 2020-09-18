var {PubSub} = require('@google-cloud/pubsub')

exports.filterSoim = (req, res) => {
  const targetTopic = 't-filtered-soim'
  const TIMEOUT = 3 * 60 * 1000 // 3 minutes
  var messageCount = 0

  const pubsub = new PubSub({projectId: 'inter-dsp-lde-dev'})
  const s_eu = pubsub.subscription('s-soim-eu')
  const s_na = pubsub.subscription('s-soim-na')
  const s_ap = pubsub.subscription('s-soim-ap')
  const forwardMessage = async (message) => {
    try {

        function filterAttributes(attributes) {
                let allowedRegions = ["NO", "US", "KR"];
                        if (attributes.region) {
                        let findRegion = (region) => {
                                return region === attributes.region;
                        };
			return allowedRegions.find(findRegion) ? true : false;
       	                }
        return false;
        }

        if (filterAttributes(message.attributes)) {
        console.log('Message Filtered-----'+ message.attributes.region)
        await pubsub.topic(targetTopic).publish(message.data, message.attributes);
        }
    } catch (error) {
      console.error(error)
    }
    messageCount += 1
    message.ack()
  }

  s_eu.on('message', forwardMessage)
  s_na.on('message', forwardMessage)
  s_ap.on('message', forwardMessage)

  setTimeout(()=>{
    s_eu.removeListener('message', forwardMessage)
    console.log("Listener for EU region removed...")
    s_na.removeListener('message', forwardMessage)
    console.log("Listener for NA region removed...")
    s_ap.removeListener('message', forwardMessage)
    console.log("Listener for AP region removed...")
    console.log(`Total messages processed..${messageCount}`)
    output = {messages: messageCount}
    res.status(200).send(JSON.stringify(output))
    }, TIMEOUT)
}
