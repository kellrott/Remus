( function( $ ) {

  $.fn.term = function( options ) {
  
    var url = options["url"];

    return this.each( function() {
      var element  = $( this );

      var pipeline = "";

      element.addClass( "terminal_main" );
      element.append("<div id='content'></div>");
      element.append( '<div><form><input type="text" ></form></div>' ); 

      var input_form = element.find( 'div:last form' );
      var input = element.find( 'div:last form input' );
      var content = element.find( "#content" );

      input.focus();
      
      
      var update_content = function( data ) {
        content.append( '<div>' + data + '</div>' );
      }; 
      
      
      input_form.submit( function(e) {
        e.preventDefault();
        e.stopPropagation();
        
        var value = input.attr( 'value' );
        

	    input.attr( 'value', '' );
	    
	    if (value.indexOf("use") == 0) {
	      pipeline = value.split(" ")[1];
	      update_content("USING: " + pipeline);
	    } else {
  	      var data = { "pipeline" : pipeline, "command" : value };
	      $.post( url, JSON.stringify(data),
            function(data) {
            	update_content(">" + value);
            	var lines = data.split("\n");
            	for (var i in lines) {
                    update_content(lines[i]);
                }
                input.animate( {scrollTop : input.offset().top}, 5);
            }
          );
        }
                
      });


    } );
  };

})( jQuery );
